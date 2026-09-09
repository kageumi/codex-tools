//! 本机 Chat Completions API → OpenAI Responses API 转换代理。
//!
//! 部分第三方服务只提供 Chat Completions API（DeepSeek、Moonshot、GLM、
//! Qwen 等），而 Codex 只讲 Responses API。本模块为这类服务启动一个只监听
//! `127.0.0.1` 的转换代理：Codex 仍然使用 Responses API 请求本机代理，
//! 代理把请求翻译成 Chat Completions 请求转发给上游服务，再把上游的流式 /
//! 非流式响应翻译回 Responses API 格式。
//!
//! 转换规则参考了社区成熟的实现（codex_deepseek_proxy、api2codex、
//! openai-responses-to-chat 等）：`input`/`instructions` → `messages`，
//! `function_call`/`function_call_output` → `tool_calls`/`tool` 消息，
//! `tools` 扁平结构 → Chat 嵌套结构，`max_output_tokens` → `max_completion_tokens`，
//! `reasoning.effort` → `reasoning_effort`；流式输出按
//! `response.output_item.added` / `response.output_text.delta` /
//! `response.function_call_arguments.delta` / `response.completed` 事件还原。
//!
//! 健壮性：请求响应头及非流式响应体分别按服务配置整体超时；
//! 上游流停滞过久转成明确的 response.failed 事件；上游忽略 stream=true
//! 直接返回 JSON 时合成等价的 SSE 流；下游长时间无事件时发送心跳注释行保活。

mod reasoning_store;
mod sse;

use crate::{
    models::{AppError, ProviderApiType, ProviderProfile},
    network::ClientCache,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{
        IntoResponse, Json, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use bytes::{Buf, BytesMut};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, oneshot};

use self::{
    reasoning_store::ReasoningStore,
    sse::{SseEventParser, utf8_lossy_slice},
};

/// 本机转换代理的监听地址（只允许本机访问）。
pub(crate) const PROXY_HOST: &str = "127.0.0.1";
/// 本机转换代理的固定端口。端口保持固定，Codex 配置里的地址跨重启也始终有效。
pub(crate) const PROXY_PORT: u16 = 27777;
/// 转换代理接受的请求体上限。Codex 会把完整会话历史（含 base64 图片）发给
/// 代理，axum 默认只允许 2MB，必须放宽，否则长对话/带图请求会被 413 拒绝。
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
/// 写入 Codex 配置的本机代理地址路径。
pub(crate) const PROXY_BASE_PATH: &str = "/v1";
/// 下游（Codex 侧）SSE 心跳间隔：上游长时间不产生事件时发送注释行，
/// 防止系统代理或客户端的空闲超时把长思考中的连接掐断。
const DOWNSTREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// 上游流式响应的停滞上限：超过该时长完全没有字节到达说明连接很可能
/// 已死亡（半开的 TCP 连接无法被及时发现），转成明确的 response.failed 事件。
const UPSTREAM_STALL_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SSE_LINE_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_DATA_BYTES: usize = 1024 * 1024;
const MAX_STREAM_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// 注册表：一个共享的本机转换代理
// ---------------------------------------------------------------------------
//
// 只存在一个监听 `127.0.0.1:27777` 的代理。上游指向当前激活的 Chat 类型服务：
// 切换服务时只原地替换共享配置，Codex 缓存的地址永远不变。

#[derive(Default)]
pub(crate) struct ChatProxyRegistry {
    single: Mutex<Option<RunningProxy>>,
}

struct RunningProxy {
    /// 本机监听端口（固定为 PROXY_PORT）。
    port: u16,
    /// 当前配置对应的服务指纹；变化时原地替换上游参数。
    fingerprint: String,
    /// 当前配置所属的服务 ID；删除该服务时清空配置，避免转发到错误的上游。
    owner: String,
    shutdown: oneshot::Sender<()>,
    /// 共享的上游配置。
    config: Arc<ProxyConfigSlot>,
    proxy_api_key: String,
}

pub(crate) struct ActivationTarget {
    pub(crate) base_url: String,
    pub(crate) proxy_api_key: Option<String>,
}

impl ChatProxyRegistry {
    /// 确保转换代理正在运行，并把上游配置切换到 `provider`。
    /// 监听地址固定为 `http://127.0.0.1:27777/v1`。
    async fn ensure(&self, provider: &ProviderProfile) -> Result<(u16, String), AppError> {
        let fingerprint = proxy_fingerprint(provider);
        let config = ProxyConfig::from_provider(provider);
        let mut single = self.single.lock().await;
        if let Some(running) = single.as_mut() {
            let owner_changed = running.owner != provider.id;
            let config_changed = running.fingerprint != fingerprint;
            if owner_changed || config_changed {
                let mut snapshot = running.config.current.write().await;
                if config_changed {
                    snapshot.config = Arc::new(config);
                    snapshot.prompt_structured_output_models =
                        Arc::new(std::sync::Mutex::new(HashSet::new()));
                    running.fingerprint = fingerprint;
                }
                snapshot.reasoning_store =
                    Arc::new(std::sync::Mutex::new(ReasoningStore::default()));
            }
            running.owner = provider.id.clone();
            return Ok((running.port, running.proxy_api_key.clone()));
        }
        let (port, shutdown, config_slot, proxy_api_key) = start_proxy(config).await?;
        *single = Some(RunningProxy {
            port,
            fingerprint,
            owner: provider.id.clone(),
            shutdown,
            config: config_slot,
            proxy_api_key: proxy_api_key.clone(),
        });
        Ok((port, proxy_api_key))
    }

    /// 服务被删除时调用：如果它正是当前代理配置的服务，清空上游配置，
    /// 让后续请求返回明确的错误而不是转发到错误的上游。监听保持运行。
    pub(crate) async fn stop(&self, provider_id: &str) {
        let mut single = self.single.lock().await;
        if let Some(running) = single.as_mut()
            && running.owner == provider_id
        {
            *running.config.current.write().await = ProxyRuntimeSnapshot {
                config: Arc::new(ProxyConfig::disabled()),
                reasoning_store: Arc::new(std::sync::Mutex::new(ReasoningStore::default())),
                prompt_structured_output_models: Arc::new(std::sync::Mutex::new(HashSet::new())),
            };
            running.fingerprint.clear();
            running.owner.clear();
        }
    }

    /// 停止全部代理（应用退出时调用）。
    pub(crate) async fn stop_all(&self) {
        if let Some(running) = self.single.lock().await.take() {
            let _ = running.shutdown.send(());
        }
    }
}

fn proxy_fingerprint(provider: &ProviderProfile) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        provider.base_url,
        provider.api_key.as_deref().unwrap_or_default(),
        serde_json::to_string(&provider.headers).unwrap_or_default(),
        provider.timeout_secs
    )
}

// ---------------------------------------------------------------------------
// 代理服务
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ProxyConfig {
    upstream_base: String,
    api_key: String,
    /// 服务商自定义请求头：配置切换时一次性构建好，请求热路径只克隆引用，
    /// 不再逐请求重复校验与组装。
    headers: reqwest::header::HeaderMap,
    /// 单次上游请求的超时（来自服务配置）：所有请求约束响应头到达时间，
    /// 非流式响应体读取另行使用同一上限。
    timeout: Duration,
}

impl ProxyConfig {
    fn from_provider(provider: &ProviderProfile) -> Self {
        Self {
            upstream_base: provider.base_url.trim_end_matches('/').to_owned(),
            api_key: provider.api_key.clone().unwrap_or_default(),
            headers: build_upstream_headers(provider.headers.clone()),
            timeout: Duration::from_secs(provider.timeout_secs.max(1)),
        }
    }

    /// 空配置：所属服务被删除后使用，任何请求都会得到明确的错误提示。
    fn disabled() -> Self {
        Self {
            upstream_base: String::new(),
            api_key: String::new(),
            headers: reqwest::header::HeaderMap::new(),
            timeout: Duration::from_secs(30),
        }
    }

    fn is_disabled(&self) -> bool {
        self.upstream_base.is_empty()
    }
}

/// 把服务配置里的自定义请求头组装成 HeaderMap。保存时已经过 validate_headers
/// 校验，这里即使遇到异常值也只跳过错误项，避免本机代理因请求头问题拒绝转发。
fn build_upstream_headers(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in pairs {
        let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        if name == reqwest::header::USER_AGENT {
            continue;
        }
        let Ok(value) = reqwest::header::HeaderValue::from_str(&value) else {
            continue;
        };
        headers.insert(name, value);
    }
    headers
}

/// 共享配置槽：切换/编辑服务时在端口不变的前提下原地替换上游参数。
/// 配置用 Arc 共享，请求热路径只复制引用而不是深拷贝整份配置。
struct ProxyConfigSlot {
    current: tokio::sync::RwLock<ProxyRuntimeSnapshot>,
}

#[derive(Clone)]
struct ProxyRuntimeSnapshot {
    config: Arc<ProxyConfig>,
    reasoning_store: Arc<std::sync::Mutex<ReasoningStore>>,
    prompt_structured_output_models: Arc<std::sync::Mutex<HashSet<String>>>,
}

impl ProxyConfigSlot {
    fn new(config: ProxyConfig) -> Arc<Self> {
        Arc::new(Self {
            current: tokio::sync::RwLock::new(ProxyRuntimeSnapshot {
                config: Arc::new(config),
                reasoning_store: Arc::new(std::sync::Mutex::new(ReasoningStore::default())),
                prompt_structured_output_models: Arc::new(std::sync::Mutex::new(HashSet::new())),
            }),
        })
    }

    async fn snapshot(
        &self,
    ) -> (
        Arc<ProxyConfig>,
        Arc<std::sync::Mutex<ReasoningStore>>,
        Arc<std::sync::Mutex<HashSet<String>>>,
    ) {
        let snapshot = self.current.read().await.clone();
        (
            snapshot.config,
            snapshot.reasoning_store,
            snapshot.prompt_structured_output_models,
        )
    }
}

struct ProxyState {
    config: Arc<ProxyConfigSlot>,
    client: ProxyClient,
    proxy_authorization: HeaderValue,
}

/// 跟随系统代理变化的独立网络客户端（不设置整体超时，允许长时间流式响应）。
struct ProxyClient {
    cached: Arc<std::sync::Mutex<Option<(crate::network::ProxySnapshot, reqwest::Client)>>>,
}

impl Default for ProxyClient {
    fn default() -> Self {
        Self {
            cached: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

impl ProxyClient {
    async fn current(&self) -> Result<reqwest::Client, AppError> {
        let cached = self.cached.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = ClientCache::cached_snapshot();
            let mut cached = cached
                .lock()
                .map_err(|_| AppError::Internal("本机转换代理的网络客户端锁已损坏。".into()))?;
            if let Some((cached_snapshot, client)) = cached.as_ref()
                && cached_snapshot == &snapshot
            {
                return Ok(client.clone());
            }
            let client = ClientCache::build_standalone(None).map_err(|error| {
                AppError::Internal(format!(
                    "无法初始化本机转换代理的网络客户端：{}",
                    error.without_url()
                ))
            })?;
            *cached = Some((snapshot, client.clone()));
            Ok(client)
        })
        .await
        .map_err(|error| AppError::Internal(format!("网络客户端初始化任务失败：{error}")))?
    }
}

/// 生产环境固定使用 PROXY_PORT；测试并行运行时每个用例分配独立端口，避免冲突。
fn bind_port() -> u16 {
    #[cfg(test)]
    {
        test_port()
    }
    #[cfg(not(test))]
    {
        PROXY_PORT
    }
}

#[cfg(test)]
fn test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(28_000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

async fn start_proxy(
    config: ProxyConfig,
) -> Result<(u16, oneshot::Sender<()>, Arc<ProxyConfigSlot>, String), AppError> {
    let config_slot = ProxyConfigSlot::new(config);
    // Each UUID v4 contributes 122 random bits after fixed version/variant bits.
    let proxy_api_key = format!(
        "{}{}{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    let state = ProxyState {
        config: config_slot.clone(),
        client: ProxyClient::default(),
        proxy_authorization: HeaderValue::from_str(&format!("Bearer {proxy_api_key}"))
            .expect("generated proxy key is a valid authorization header"),
    };
    let listener = tokio::net::TcpListener::bind((PROXY_HOST, bind_port()))
        .await
        .map_err(|error| {
            AppError::Internal(format!(
                "无法在本机 {PROXY_HOST}:{PROXY_PORT} 启动转换代理（{error}）。请确认端口 27777 未被其他程序占用后重试。"
            ))
        })?;
    let address = listener
        .local_addr()
        .map_err(|error| AppError::Internal(format!("无法读取本机转换代理端口：{error}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let app = Router::new()
        .route("/v1/responses", post(handle_responses))
        .route("/v1/models", get(handle_models))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(Arc::new(state));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((address.port(), shutdown_tx, config_slot, proxy_api_key))
}

/// 服务只提供 Chat Completions API 时，写入 Codex 的 base_url 是本机代理；
/// 直连 Responses API 的服务则写服务商地址本身。
pub(crate) async fn effective_base_url(
    provider: &ProviderProfile,
    registry: &ChatProxyRegistry,
) -> Result<ActivationTarget, AppError> {
    match provider.api_type {
        ProviderApiType::Responses => Ok(ActivationTarget {
            base_url: provider.base_url.clone(),
            proxy_api_key: None,
        }),
        ProviderApiType::Chat => {
            let (port, proxy_api_key) = registry.ensure(provider).await?;
            Ok(ActivationTarget {
                base_url: format!("http://{PROXY_HOST}:{port}{PROXY_BASE_PATH}"),
                proxy_api_key: Some(proxy_api_key),
            })
        }
    }
}

fn apply_upstream_headers(
    mut request: reqwest::RequestBuilder,
    config: &ProxyConfig,
) -> reqwest::RequestBuilder {
    if !config.api_key.is_empty() {
        request = request.bearer_auth(&config.api_key);
    }
    if !config.headers.is_empty() {
        request = request.headers(config.headers.clone());
    }
    request
}

/// 发送上游 Chat Completions 请求，并把连接/超时错误翻译成给 Codex 的错误响应。
///
/// 转换代理的网络客户端为了长时间流式响应特意不设总超时，这里约束响应头
/// 到达时间；非流式响应体在调用方另行约束整体读取时间。
async fn send_chat_request(
    client: &reqwest::Client,
    url: &str,
    config: &ProxyConfig,
    body: &Value,
) -> Result<reqwest::Response, Box<Response>> {
    let request = apply_upstream_headers(client.post(url), config).json(body);
    let result = match tokio::time::timeout(config.timeout, request.send()).await {
        Ok(result) => result,
        Err(_) => {
            return Err(Box::new(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                &format!(
                    "等待上游服务返回响应超时（{} 秒）。",
                    config.timeout.as_secs()
                ),
            )));
        }
    };
    result.map_err(|error| {
        if error.is_timeout() {
            Box::new(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                &format!("等待上游服务响应超时（{} 秒）。", config.timeout.as_secs()),
            ))
        } else {
            Box::new(error_response(
                StatusCode::BAD_GATEWAY,
                &format!("无法连接上游服务：{error}"),
            ))
        }
    })
}

/// 读取上游错误响应的响应体；读取失败/超时时返回空串，由调用方兜底文案。
async fn read_error_body(response: reqwest::Response, config: &ProxyConfig) -> String {
    match tokio::time::timeout(
        config.timeout,
        crate::provider_http::read_response_body_limited(
            response,
            crate::provider_http::MAX_UPSTREAM_BODY_BYTES,
        ),
    )
    .await
    {
        Ok(Ok(body)) => String::from_utf8_lossy(&body).into_owned(),
        _ => String::new(),
    }
}

/// 只在 Content-Type 明确声明 SSE 时实时转发。缺失或含糊时先有界读取并嗅探，
/// 避免把无响应头的完整 JSON 当作 SSE 后静默产出空 response。
fn is_event_stream(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        })
}

/// 给 Codex 的 SSE 响应统一入口：禁用一切缓冲，保证事件逐条实时到达。
fn sse_response(
    stream: impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>> + Send + 'static,
) -> Response {
    let mut response = Sse::new(stream).into_response();
    response
        .headers_mut()
        .insert("Cache-Control", HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert("X-Accel-Buffering", HeaderValue::from_static("no"));
    response
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let mut response = Json(json!({
        "error": {"message": message, "type": "upstream_error", "param": null, "code": status.as_u16().to_string()}
    }))
    .into_response();
    *response.status_mut() = status;
    response
}

fn invalid_request_response(message: &str) -> Response {
    let mut response = Json(json!({
        "error": {"message": message, "type": "invalid_request_error", "param": null, "code": "invalid_request"}
    }))
    .into_response();
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response
}

/// 校验调用方凭证：只有本应用为当前代理运行实例生成的 Key 可以访问，
/// 防止本机其他进程或网页借用代理消耗用户的真实上游额度。
fn is_authorized(headers: &HeaderMap, expected: &HeaderValue) -> bool {
    headers.get(reqwest::header::AUTHORIZATION) == Some(expected)
}

fn unauthorized_response() -> Response {
    error_response(
        StatusCode::UNAUTHORIZED,
        "缺少或错误的代理访问凭证，请求已拒绝。",
    )
}

async fn handle_models(State(state): State<Arc<ProxyState>>, headers: HeaderMap) -> Response {
    if !is_authorized(&headers, &state.proxy_authorization) {
        return unauthorized_response();
    }
    let (config, _, _) = state.config.snapshot().await;
    if config.is_disabled() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "本机转换代理尚未配置上游服务，请先在应用中切换一个 Chat Completions 服务。",
        );
    }
    let client = match state.client.current().await {
        Ok(client) => client,
        Err(error) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let url = crate::provider_http::endpoint_for(&config.upstream_base, "models");
    // /models 是非流式请求，需要整体超时：转换代理的网络客户端为了
    // 长时间流式响应特意不设总超时，若不在这里兜底，上游建连后一直
    // 不返回会让 Codex 侧的模型列表请求无限悬挂。超时沿用服务配置。
    let fetch = async {
        let response = apply_upstream_headers(client.get(&url), &config)
            .send()
            .await
            .map_err(|error| format!("无法连接上游服务获取模型列表：{error}"))?;
        let status = response.status();
        let bytes = crate::provider_http::read_response_body_limited(
            response,
            crate::provider_http::MAX_UPSTREAM_BODY_BYTES,
        )
        .await
        .map_err(|error| error.to_string())?;
        let body = serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| "上游服务返回的模型列表不是有效 JSON。".to_string())?;
        Ok::<_, String>((status, body))
    };
    match tokio::time::timeout(config.timeout, fetch).await {
        Ok(Ok((status, body))) => (status, Json(body)).into_response(),
        Ok(Err(message)) => error_response(StatusCode::BAD_GATEWAY, &message),
        Err(_) => error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "等待上游服务返回模型列表超时。",
        ),
    }
}

async fn handle_responses(
    State(state): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body_bytes: Bytes,
) -> Response {
    if !is_authorized(&headers, &state.proxy_authorization) {
        return unauthorized_response();
    }
    let body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(body @ Value::Object(_)) => body,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "请求体不是有效的 JSON 对象，无法转换为 Chat Completions 请求。",
            );
        }
        Ok(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "请求体必须是 JSON 对象，无法转换为 Chat Completions 请求。",
            );
        }
    };
    drop(body_bytes);
    if let Err(message) = validate_supported_responses_request(&body) {
        return invalid_request_response(&message);
    }
    // 先校验代理配置，避免在未配置上游时做无用的请求翻译。
    let (config, reasoning_store, structured_output_capabilities) = state.config.snapshot().await;
    if config.is_disabled() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "本机转换代理尚未配置上游服务，请先在应用中切换一个 Chat Completions 服务。",
        );
    }
    let stream_requested = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let response_metadata = ResponseMetadata::from_request(&body);
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let native_chat_body = responses_to_chat_body(&body, &reasoning_store);
    let structured_output = native_chat_body.get("response_format").is_some();
    let structured_schema = native_chat_body
        .pointer("/response_format/json_schema/schema")
        .cloned();
    let format_kind = native_chat_body
        .pointer("/response_format/type")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let capability_key = format!("{model}\n{format_kind}");
    let use_prompt_structured_output = structured_output
        && structured_output_capabilities
            .lock()
            .is_ok_and(|models| models.contains(&capability_key));
    let degraded_body = if use_prompt_structured_output {
        Some(degrade_structured_output(&native_chat_body))
    } else {
        None
    };
    let chat_body = degraded_body.as_ref().unwrap_or(&native_chat_body);
    let client = match state.client.current().await {
        Ok(client) => client,
        Err(error) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let url = crate::provider_http::endpoint_for(&config.upstream_base, "chat/completions");
    let mut response = match send_chat_request(&client, &url, &config, chat_body).await {
        Ok(response) => response,
        Err(error_response) => return *error_response,
    };
    let mut learn_prompt_capability = false;
    if !response.status().is_success() {
        let status = response.status();
        let detail = read_error_body(response, &config).await;
        // Unknown OpenAI-compatible Chat endpoints are capability-probed with
        // the standard response_format request. A validation rejection before
        // any output permits one prompt-based retry; a successful retry learns
        // that capability for this exact upstream/model pair.
        if structured_output
            && !use_prompt_structured_output
            && matches!(status.as_u16(), 400 | 422)
        {
            let degraded = degrade_structured_output(&native_chat_body);
            match send_chat_request(&client, &url, &config, &degraded).await {
                Ok(retry) => {
                    if !retry.status().is_success() {
                        let status = retry.status();
                        let retry_detail = read_error_body(retry, &config).await;
                        return upstream_error_response(status, &retry_detail);
                    }
                    learn_prompt_capability = true;
                    response = retry;
                }
                Err(error_response) => return *error_response,
            }
        } else {
            return upstream_error_response(status, &detail);
        }
    }
    let response_id = format!("resp_{}", uuid::Uuid::new_v4().simple());
    let created_at = chrono::Utc::now().timestamp();
    if stream_requested {
        // 上游忽略了 stream=true、直接返回完整 JSON（部分第三方网关在流式
        // 请求出错时也会用 200 + JSON 报错）：按内容兜底，保证 Codex 侧始终
        // 收到符合协议的 SSE 流或明确的错误。
        if !is_event_stream(&response) {
            return fallback_non_sse_stream(
                response,
                &config,
                ResponseTranslationContext {
                    response_id,
                    model,
                    created_at,
                    store: reasoning_store,
                    structured_output,
                    structured_schema,
                    metadata: response_metadata.clone(),
                    capability_learning: learn_prompt_capability.then(|| CapabilityLearning {
                        cache: structured_output_capabilities.clone(),
                        key: capability_key.clone(),
                    }),
                },
            )
            .await;
        }
        let stream = translate_stream(
            response,
            ResponseTranslationContext {
                response_id,
                model,
                created_at,
                store: reasoning_store,
                structured_output,
                structured_schema,
                metadata: response_metadata.clone(),
                capability_learning: learn_prompt_capability.then(|| CapabilityLearning {
                    cache: structured_output_capabilities.clone(),
                    key: capability_key.clone(),
                }),
            },
        );
        sse_response(stream)
    } else {
        let chat_response = match tokio::time::timeout(
            config.timeout,
            crate::provider_http::read_response_body_limited(
                response,
                crate::provider_http::MAX_UPSTREAM_BODY_BYTES,
            ),
        )
        .await
        {
            Ok(Ok(body)) => match serde_json::from_slice::<Value>(&body) {
                Ok(value) => value,
                Err(_) => {
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "上游服务返回的响应不是有效 JSON。",
                    );
                }
            },
            Err(_) => {
                return error_response(StatusCode::GATEWAY_TIMEOUT, "读取上游服务响应超时。");
            }
            Ok(Err(_)) => return error_response(StatusCode::BAD_GATEWAY, "读取上游服务响应失败。"),
        };
        let translated = match chat_to_responses_body_with_schema(
            &chat_response,
            &response_id,
            &model,
            created_at,
            &reasoning_store,
            structured_output,
            structured_schema.as_ref(),
        ) {
            Ok(translated) => translated,
            Err(message) => return error_response(StatusCode::BAD_GATEWAY, &message),
        };
        let mut translated = translated;
        response_metadata.apply(&mut translated);
        if learn_prompt_capability {
            record_prompt_structured_output_capability(
                &structured_output_capabilities,
                capability_key,
            );
        }
        Json(translated).into_response()
    }
}

struct ResponseTranslationContext {
    response_id: String,
    model: String,
    created_at: i64,
    store: Arc<std::sync::Mutex<ReasoningStore>>,
    structured_output: bool,
    structured_schema: Option<Value>,
    metadata: ResponseMetadata,
    capability_learning: Option<CapabilityLearning>,
}

#[derive(Clone)]
struct ResponseMetadata {
    values: Map<String, Value>,
}

impl ResponseMetadata {
    fn from_request(body: &Value) -> Self {
        let mut values = Map::new();
        for (key, default) in [
            ("instructions", Value::Null),
            ("metadata", json!({})),
            ("parallel_tool_calls", Value::Bool(true)),
            ("tool_choice", Value::String("auto".into())),
            ("tools", json!([])),
            ("temperature", Value::Null),
            ("top_p", Value::Null),
            ("max_output_tokens", Value::Null),
            ("reasoning", json!({"effort": null, "summary": null})),
            ("text", json!({"format": {"type": "text"}})),
            ("store", Value::Bool(true)),
            ("truncation", Value::String("disabled".into())),
            ("user", Value::Null),
        ] {
            values.insert(key.into(), body.get(key).cloned().unwrap_or(default));
        }
        Self { values }
    }

    fn apply(&self, response: &mut Value) {
        if let Some(response) = response.as_object_mut() {
            response.extend(self.values.clone());
        }
    }
}

impl Default for ResponseMetadata {
    fn default() -> Self {
        Self::from_request(&Value::Null)
    }
}

struct CapabilityLearning {
    cache: Arc<std::sync::Mutex<HashSet<String>>>,
    key: String,
}

fn record_prompt_structured_output_capability(
    cache: &std::sync::Mutex<HashSet<String>>,
    key: String,
) {
    const MAX_CAPABILITIES: usize = 2048;
    if let Ok(mut capabilities) = cache.lock() {
        if capabilities.len() >= MAX_CAPABILITIES {
            capabilities.clear();
        }
        capabilities.insert(key);
    }
}

/// 上游对流式请求返回了非 SSE 响应时的兜底：
/// - 完整 JSON 补全（上游忽略 stream=true）：合成为等价的 Responses SSE 事件流；
/// - JSON 错误（网关用 200 报错）：转成明确的错误响应；
/// - 其他内容：返回 502，不伪装成正常响应。
async fn fallback_non_sse_stream(
    response: reqwest::Response,
    config: &ProxyConfig,
    context: ResponseTranslationContext,
) -> Response {
    let text = match tokio::time::timeout(
        config.timeout,
        crate::provider_http::read_response_body_limited(
            response,
            crate::provider_http::MAX_UPSTREAM_BODY_BYTES,
        ),
    )
    .await
    {
        Ok(Ok(body)) => String::from_utf8_lossy(&body).into_owned(),
        Err(_) => {
            return error_response(StatusCode::GATEWAY_TIMEOUT, "读取上游服务响应超时。");
        }
        Ok(Err(_)) => {
            return error_response(StatusCode::BAD_GATEWAY, "读取上游服务响应失败。");
        }
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) if text.lines().any(|line| line.starts_with("data:")) => {
            let pending = translate_buffered_sse(&text, context);
            let stream = futures_util::stream::iter(
                pending
                    .into_iter()
                    .map(|event| Ok::<_, std::convert::Infallible>(into_axum_event(event))),
            );
            return sse_response(stream);
        }
        Err(_) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                &format!("上游服务返回了无法识别的响应：{}", truncate(&text, 200)),
            );
        }
    };
    if value.get("error").is_some() {
        return upstream_error_response(StatusCode::BAD_GATEWAY, &text);
    }
    if value.get("choices").is_none() {
        return error_response(
            StatusCode::BAD_GATEWAY,
            "上游服务返回的响应既不是 SSE 流也不是补全结果。",
        );
    }
    let mut translator = StreamTranslator::new_with_metadata(
        &context.response_id,
        &context.model,
        context.created_at,
        context.structured_output,
        context.structured_schema,
        context.metadata,
    );
    let created = translator.stub("response.created", "in_progress");
    let in_progress = translator.stub("response.in_progress", "in_progress");
    let mut pending = vec![
        sequenced_sse_event(&mut translator.sequence, "response.created", created),
        sequenced_sse_event(
            &mut translator.sequence,
            "response.in_progress",
            in_progress,
        ),
    ];
    // 把一次性补全结果包装成单个分片，复用同一套翻译逻辑生成完整事件序列。
    let chunk = completion_to_chunk(&value);
    let failed = translator.push_chunk(&chunk, &mut pending).is_some();
    let completed = !failed && translator.finish(&mut pending, &context.store);
    if completed && let Some(learning) = context.capability_learning {
        record_prompt_structured_output_capability(&learning.cache, learning.key);
    }
    if !completed && !translator.has_failed_event {
        translator.fail(&mut pending, "无法转换上游 Chat Completions 响应。");
    }
    let stream = futures_util::stream::iter(
        pending
            .into_iter()
            .map(|event| Ok::<_, std::convert::Infallible>(into_axum_event(event))),
    );
    sse_response(stream)
}

/// 部分兼容服务会返回合法 SSE 正文却遗漏 Content-Type。此路径先有界读取完整
/// 响应，再复用同一个解析器和状态机，牺牲实时性但不牺牲协议正确性。
fn translate_buffered_sse(text: &str, context: ResponseTranslationContext) -> Vec<PendingEvent> {
    let mut translator = StreamTranslator::new_with_metadata(
        &context.response_id,
        &context.model,
        context.created_at,
        context.structured_output,
        context.structured_schema,
        context.metadata,
    );
    let created = translator.stub("response.created", "in_progress");
    let in_progress = translator.stub("response.in_progress", "in_progress");
    let mut pending = vec![
        sequenced_sse_event(&mut translator.sequence, "response.created", created),
        sequenced_sse_event(
            &mut translator.sequence,
            "response.in_progress",
            in_progress,
        ),
    ];
    let mut parser = SseEventParser::default();
    let mut failed = false;
    let mut done = false;
    for line in text.split('\n') {
        match parser.push_line(line) {
            Ok(Some(data)) => {
                let stop =
                    dispatch_data(&mut translator, &data, &mut pending, &mut failed, &mut done);
                if stop {
                    break;
                }
            }
            Err(()) => {
                failed = true;
                translator.fail(&mut pending, "上游 SSE 事件数据超过允许大小。");
                break;
            }
            Ok(None) => {}
        }
    }
    if !done
        && !failed
        && let Some(data) = parser.finish()
    {
        dispatch_data(&mut translator, &data, &mut pending, &mut failed, &mut done);
    }
    if failed {
        if !translator.has_failed_event {
            translator.fail(&mut pending, "无法解析上游 SSE 响应。");
        }
    } else {
        let completed = translator.finish(&mut pending, &context.store);
        if completed && let Some(learning) = context.capability_learning {
            record_prompt_structured_output_capability(&learning.cache, learning.key);
        }
    }
    pending
}

/// 把非流式 Chat Completions 补全结果包装成流式分片形状，
/// 供 StreamTranslator 复用同一套事件生成逻辑。
/// 非流式的 tool_calls 不带 index，这里按位置补上，避免多个调用被折叠成一个。
fn completion_to_chunk(completion: &Value) -> Value {
    let message = completion
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .cloned()
        .unwrap_or(Value::Null);
    let mut delta = Map::new();
    delta.insert("role".into(), Value::String("assistant".into()));
    if let Some(content) = message.get("content").filter(|content| !content.is_null()) {
        delta.insert("content".into(), content.clone());
    }
    if let Some(parsed) = message.get("parsed").filter(|value| !value.is_null()) {
        delta.insert("parsed".into(), parsed.clone());
    }
    if let Some(refusal) = message.get("refusal").filter(|value| !value.is_null()) {
        delta.insert("refusal".into(), refusal.clone());
    }
    for key in [
        "reasoning_content",
        "reasoning",
        "analysis",
        "reasoning_details",
    ] {
        if let Some(reasoning) = message.get(key).filter(|value| !value.is_null()) {
            delta.insert(key.into(), reasoning.clone());
        }
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        let indexed = tool_calls
            .iter()
            .enumerate()
            .map(|(index, call)| {
                let mut call = call.clone();
                if let Some(map) = call.as_object_mut() {
                    map.insert("index".into(), json!(index));
                }
                call
            })
            .collect::<Vec<_>>();
        delta.insert("tool_calls".into(), Value::Array(indexed));
    }
    if let Some(function_call) = message.get("function_call") {
        delta.insert("function_call".into(), function_call.clone());
    }
    let mut chunk = json!({"choices": [{"index": 0, "delta": Value::Object(delta)}]});
    if let Some(model) = completion.get("model") {
        chunk["model"] = model.clone();
    }
    if let Some(usage) = completion.get("usage") {
        chunk["usage"] = usage.clone();
    }
    if let Some(finish_reason) = completion
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
    {
        chunk["choices"][0]["finish_reason"] = finish_reason.clone();
    }
    chunk
}

/// 上游返回非成功状态时，尽量把 Chat 风格的错误原样带回给 Codex。
fn upstream_error_response(status: StatusCode, detail: &str) -> Response {
    if let Ok(value) = serde_json::from_str::<Value>(detail)
        && value.get("error").is_some()
    {
        return (status, Json(value)).into_response();
    }
    let message = if detail.trim().is_empty() {
        format!("上游服务返回 HTTP {}。", status.as_u16())
    } else {
        format!(
            "上游服务返回 HTTP {}：{}",
            status.as_u16(),
            truncate(detail, 500)
        )
    };
    error_response(status, &message)
}

fn truncate(value: &str, limit: usize) -> String {
    // 用 char_indices 单趟定位第 limit 个字符的字节边界，
    // 避免为整个字符串分配字符数组（上游错误详情可能很大）。
    let cut = value
        .char_indices()
        .enumerate()
        .find_map(|(count, (index, _))| (count == limit).then_some(index));
    let Some(cut) = cut else {
        return value.to_owned();
    };
    let mut output = String::with_capacity(cut + 1);
    output.push_str(&value[..cut]);
    output.push('…');
    output
}

/// 处理一条完整的事件数据。返回 `true` 表示遇到 [DONE] 或上游错误，应停止读取。
fn dispatch_data(
    translator: &mut StreamTranslator,
    data: &str,
    pending: &mut Vec<PendingEvent>,
    failed: &mut bool,
    done: &mut bool,
) -> bool {
    if data == "[DONE]" {
        *done = true;
        return true;
    }
    match serde_json::from_str::<Value>(data) {
        Ok(chunk) => {
            if translator.push_chunk(&chunk, pending).is_some() {
                *failed = true;
                return true;
            }
        }
        Err(_) => {
            translator.fail(pending, "上游流式响应包含无效 JSON。");
            *failed = true;
            return true;
        }
    }
    false
}

fn translate_stream(
    response: reqwest::Response,
    context: ResponseTranslationContext,
) -> impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingEvent>(64);
    tokio::spawn(async move {
        let mut translator = StreamTranslator::new_with_metadata(
            &context.response_id,
            &context.model,
            context.created_at,
            context.structured_output,
            context.structured_schema,
            context.metadata,
        );
        let created = translator.stub("response.created", "in_progress");
        let in_progress = translator.stub("response.in_progress", "in_progress");
        let mut pending = vec![
            sequenced_sse_event(&mut translator.sequence, "response.created", created),
            sequenced_sse_event(
                &mut translator.sequence,
                "response.in_progress",
                in_progress,
            ),
        ];
        // 立即把 created/in_progress 发给客户端，让 Codex 尽早感知连接已建立；
        // 也避免上游（或系统代理）建连较慢时长时间没有任何事件。
        if !flush_events(&tx, &mut pending).await {
            return;
        }

        let mut parser = SseEventParser::default();
        // 预分配接收缓冲，避免逐块扩容；行提取用游标定位，避免每行整体搬运剩余缓冲。
        let mut buffer = BytesMut::with_capacity(8 * 1024);
        let mut stream = response.bytes_stream();
        let mut failed = false;
        let mut done = false;
        let mut received_bytes = 0usize;
        let mut failure_message = "读取上游流式响应失败，连接可能已中断。";
        loop {
            // 停滞检测：上游长时间没有任何字节到达，视为连接已死亡，
            // 转成明确的失败事件而不是无限悬挂。
            let chunk = match tokio::time::timeout(UPSTREAM_STALL_TIMEOUT, stream.next()).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(_) => {
                    failed = true;
                    failure_message = "上游服务长时间没有返回数据，连接已超时中断。";
                    break;
                }
            };
            match chunk {
                Ok(bytes) => {
                    received_bytes = received_bytes.saturating_add(bytes.len());
                    if received_bytes > MAX_STREAM_RESPONSE_BYTES {
                        failed = true;
                        failure_message = "上游流式响应超过允许大小。";
                        break;
                    }
                    buffer.extend_from_slice(&bytes);
                }
                Err(_) => {
                    failed = true;
                    break;
                }
            }
            let mut start = 0usize;
            while let Some(relative) = buffer[start..].iter().position(|&byte| byte == b'\n') {
                let position = start + relative;
                let line = utf8_lossy_slice(&buffer[start..position]);
                start = position + 1;
                match parser.push_line(line.as_ref()) {
                    Ok(Some(data)) => {
                        let stop = dispatch_data(
                            &mut translator,
                            &data,
                            &mut pending,
                            &mut failed,
                            &mut done,
                        );
                        if stop {
                            break;
                        }
                    }
                    Err(()) => {
                        failed = true;
                        failure_message = "上游 SSE 事件数据超过允许大小。";
                        break;
                    }
                    _ => {}
                }
                if pending.len() >= 32 && !flush_events(&tx, &mut pending).await {
                    return;
                }
            }
            if !done && !failed && buffer.len().saturating_sub(start) > MAX_SSE_LINE_BUFFER_BYTES {
                failed = true;
                failure_message = "上游 SSE 未换行数据超过允许大小。";
            }
            if !done && !failed && start > 0 {
                // 已消费的行一次性移除（每块只搬运一次剩余部分）；
                // 结束时不搬，直接丢弃。
                buffer.advance(start);
            }
            if !flush_events(&tx, &mut pending).await {
                return;
            }
            if done || failed {
                break;
            }
        }
        if !done && !failed {
            // 上游没有发送 [DONE] 就断开：先把残留的未换行行喂给解析器，
            // 再分发解析器里累积的数据，保证最后一个事件不丢失。
            if !buffer.is_empty() {
                match parser.push_line(utf8_lossy_slice(&buffer).as_ref()) {
                    Ok(Some(data)) => {
                        dispatch_data(&mut translator, &data, &mut pending, &mut failed, &mut done);
                    }
                    Err(()) => {
                        failed = true;
                        failure_message = "上游 SSE 事件数据超过允许大小。";
                    }
                    Ok(None) => {}
                }
            }
            if !failed && let Some(data) = parser.finish() {
                dispatch_data(&mut translator, &data, &mut pending, &mut failed, &mut done);
            }
        }
        if failed {
            // 上游已在流中报告错误（fail 事件已发出）或连接中断。
            if !translator.has_failed_event {
                translator.fail(&mut pending, failure_message);
            }
        } else {
            let completed = translator.finish(&mut pending, &context.store);
            if completed && let Some(learning) = context.capability_learning {
                record_prompt_structured_output_capability(&learning.cache, learning.key);
            }
        }
        flush_events(&tx, &mut pending).await;
        // 发送端随任务结束被丢弃，接收端流随之结束。
    });
    let keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + DOWNSTREAM_KEEPALIVE_INTERVAL,
        DOWNSTREAM_KEEPALIVE_INTERVAL,
    );
    futures_util::stream::unfold((rx, keepalive), |(mut rx, mut keepalive)| async move {
        // 优先转发真实事件；长时间没有事件时发送 SSE 注释行心跳，
        // 防止任何一层的空闲超时掐断长思考中的连接。
        tokio::select! {
            biased;
            event = rx.recv() => event.map(|event| {
                (
                    Ok::<Event, std::convert::Infallible>(into_axum_event(event)),
                    (rx, keepalive),
                )
            }),
            _ = keepalive.tick() => Some((
                Ok(Event::default().comment("keep-alive")),
                (rx, keepalive),
            )),
        }
    })
}

async fn flush_events(
    tx: &tokio::sync::mpsc::Sender<PendingEvent>,
    pending: &mut Vec<PendingEvent>,
) -> bool {
    for event in pending.drain(..) {
        // 优先无等待发送；通道满时才让出（背压），减少逐事件 await 的开销。
        match tx.try_send(event) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                if tx.send(event).await.is_err() {
                    return false;
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        }
    }
    true
}

/// 翻译过程中暂存的 SSE 事件；发送前再转成 axum 的 `Event`。
/// 单独定义是为了让单元测试可以直接检查事件类型与载荷。
struct PendingEvent {
    event_type: &'static str,
    data: String,
}

fn sse_event(event_type: &'static str, payload: &Value) -> PendingEvent {
    PendingEvent {
        event_type,
        data: serde_json::to_string(payload).unwrap_or_else(|_| "{}".into()),
    }
}

fn sequenced_sse_event(
    sequence: &mut u32,
    event_type: &'static str,
    mut payload: Value,
) -> PendingEvent {
    if let Some(object) = payload.as_object_mut() {
        object.insert("sequence_number".into(), json!(*sequence));
    }
    *sequence = sequence.saturating_add(1);
    sse_event(event_type, &payload)
}

/// 逐 token 的热路径事件：直接构建 JSON 字符串，避免中间 `Value` 分配。
/// `item_id` 是代理生成的十六进制 id（无需转义）；`delta` 用 serde_json 转义。
fn text_delta_event(
    event_type: &'static str,
    item_id: &str,
    output_index: usize,
    content_index: usize,
    sequence: u32,
    delta: &str,
) -> PendingEvent {
    let delta = serde_json::to_string(delta).unwrap_or_else(|_| "\"\"".to_owned());
    PendingEvent {
        event_type,
        data: format!(
            "{{\"type\":\"{event_type}\",\"item_id\":\"{item_id}\",\"output_index\":{output_index},\"content_index\":{content_index},\"sequence_number\":{sequence},\"delta\":{delta},\"logprobs\":[]}}"
        ),
    }
}

fn arguments_delta_event(
    item_id: &str,
    output_index: usize,
    sequence: u32,
    delta: &str,
) -> PendingEvent {
    let delta = serde_json::to_string(delta).unwrap_or_else(|_| "\"\"".to_owned());
    PendingEvent {
        event_type: "response.function_call_arguments.delta",
        data: format!(
            "{{\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"{item_id}\",\"output_index\":{output_index},\"sequence_number\":{sequence},\"delta\":{delta}}}"
        ),
    }
}

fn into_axum_event(pending: PendingEvent) -> Event {
    Event::default()
        .event(pending.event_type)
        .data(pending.data)
}

// ---------------------------------------------------------------------------
// 请求转换：Responses API → Chat Completions
// ---------------------------------------------------------------------------

fn validate_supported_responses_request(body: &Value) -> Result<(), String> {
    if body
        .get("model")
        .and_then(Value::as_str)
        .is_none_or(|model| model.trim().is_empty() || model.len() > 512)
    {
        return Err("Responses 请求缺少有效的 model。".into());
    }
    if body.get("stream").is_some_and(|value| !value.is_boolean()) {
        return Err("Responses stream 必须是布尔值。".into());
    }
    if body
        .get("previous_response_id")
        .is_some_and(|value| !value.is_null())
        || body
            .get("conversation")
            .is_some_and(|value| !value.is_null())
        || body.get("background").and_then(Value::as_bool) == Some(true)
    {
        return Err("Chat Completions 转换不支持服务端会话状态，请发送完整 input 历史。".into());
    }
    if let Some(format) = body.get("text").and_then(|text| text.get("format")) {
        match format.get("type").and_then(Value::as_str) {
            None | Some("text" | "json_object") => {}
            Some("json_schema") => {
                let schema = format
                    .get("schema")
                    .ok_or_else(|| "Responses text.format.json_schema 缺少 schema。".to_string())?;
                if !jsonschema::meta::is_valid(schema) {
                    return Err("Responses text.format 包含无效的 JSON Schema。".into());
                }
                if schema.get("type").and_then(Value::as_str) != Some("object") {
                    return Err("Responses 结构化输出仅支持 object 根 Schema。".into());
                }
            }
            Some(format_type) => {
                return Err(format!(
                    "Chat Completions 转换不支持 text.format 类型：{format_type}。"
                ));
            }
        }
    }
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        for tool in tools
            .iter()
            .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
        {
            if tool
                .get("name")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err("Responses function 工具缺少 name。".into());
            }
            if let Some(parameters) = tool.get("parameters")
                && !jsonschema::meta::is_valid(parameters)
            {
                return Err("Responses function 工具包含无效的 parameters Schema。".into());
            }
        }
    }
    if let Some(choice) = body.get("tool_choice") {
        let function_names = body
            .get("tools")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<HashSet<_>>();
        let supported = match choice {
            Value::String(value) => matches!(value.as_str(), "auto" | "none" | "required"),
            Value::Object(map) => {
                map.get("type").and_then(Value::as_str) == Some("function")
                    && map
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty())
            }
            _ => false,
        };
        if !supported {
            return Err("Chat Completions 转换不支持此 tool_choice。".into());
        }
        if choice.as_str() == Some("required") && function_names.is_empty() {
            return Err("tool_choice=required，但没有可转换的 function 工具。".into());
        }
        if let Some(name) = choice.get("name").and_then(Value::as_str)
            && !function_names.contains(name)
        {
            return Err("tool_choice 指定的 function 未出现在工具列表中。".into());
        }
    }
    if body
        .get("input")
        .is_some_and(|input| !input.is_string() && !input.is_array())
    {
        return Err("Responses input 必须是字符串或条目数组。".into());
    }
    if let Some(items) = body.get("input").and_then(Value::as_array) {
        for item in items {
            let Some(map) = item.as_object() else {
                if item.is_string() {
                    continue;
                }
                return Err("Responses input 包含不支持的条目。".into());
            };
            match map.get("type").and_then(Value::as_str).unwrap_or("message") {
                "message" | "" => {
                    if map.get("role").and_then(Value::as_str).is_none_or(|role| {
                        !matches!(role, "system" | "developer" | "user" | "assistant")
                    }) {
                        return Err("Responses message 包含不支持的 role。".into());
                    }
                    if !map.get("content").is_some_and(|content| {
                        content.is_string() || content.is_array() || content.is_null()
                    }) {
                        return Err("Responses message 缺少有效的 content。".into());
                    }
                }
                // Reasoning items carry no Chat-visible content. Provider
                // reasoning needed for continuation is restored from the
                // bounded item/call-id store instead of exposing private text.
                "reasoning" => {}
                "function_call" => {
                    if map
                        .get("call_id")
                        .or_else(|| map.get("id"))
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                        || map
                            .get("name")
                            .and_then(Value::as_str)
                            .is_none_or(str::is_empty)
                        || !map.get("arguments").is_some_and(Value::is_string)
                    {
                        return Err(
                            "Responses function_call 缺少有效的 call_id、name 或 arguments。"
                                .into(),
                        );
                    }
                }
                "function_call_output" => {
                    if map
                        .get("call_id")
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                    {
                        return Err("Responses function_call_output 缺少 call_id。".into());
                    }
                    if !map.contains_key("output") {
                        return Err("Responses function_call_output 缺少 output。".into());
                    }
                }
                item_type => {
                    return Err(format!(
                        "Chat Completions 转换不支持 Responses input 类型：{item_type}。"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn responses_to_chat_body(body: &Value, store: &std::sync::Mutex<ReasoningStore>) -> Value {
    let mut chat = Map::new();
    if let Some(model) = body.get("model").cloned() {
        chat.insert("model".into(), model);
    }
    let store = reasoning_lookup_for_body(body, store);
    chat.insert(
        "messages".into(),
        Value::Array(input_to_messages(body, &store)),
    );
    let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    if stream {
        chat.insert("stream".into(), Value::Bool(true));
        // 请求用量统计，供 response.completed 事件回填；不支持的平台会忽略该字段。
        chat.insert("stream_options".into(), json!({"include_usage": true}));
    }
    let tools = body
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| convert_tools(tools))
        .unwrap_or_default();
    let has_tools = !tools.is_empty();
    if has_tools {
        chat.insert("tools".into(), Value::Array(tools));
        if let Some(parallel) = body.get("parallel_tool_calls").cloned() {
            chat.insert("parallel_tool_calls".into(), parallel);
        }
    }
    if has_tools && let Some(tool_choice) = body.get("tool_choice") {
        let converted = convert_tool_choice(tool_choice);
        // 默认的 auto 不需要显式发送，部分平台对不认识的参数会直接报错。
        if converted != Value::String("auto".into()) {
            chat.insert("tool_choice".into(), converted);
        }
    }
    if let Some(max_output_tokens) = body.get("max_output_tokens") {
        chat.insert("max_completion_tokens".into(), max_output_tokens.clone());
    }
    for key in ["temperature", "top_p", "store", "metadata", "user"] {
        if let Some(value) = body.get(key).filter(|value| !value.is_null()) {
            chat.insert(key.into(), value.clone());
        }
    }
    if let Some(reasoning) = body.get("reasoning").and_then(Value::as_object)
        && let Some(effort) = reasoning.get("effort").and_then(Value::as_str)
        && !effort.is_empty()
    {
        chat.insert("reasoning_effort".into(), Value::String(effort.into()));
    }
    // Responses text.format → Chat response_format，保留 schema 与 strict 语义。
    if let Some(format) = body.get("text").and_then(|text| text.get("format")) {
        match format.get("type").and_then(Value::as_str) {
            Some("json_schema") => {
                let mut json_schema = Map::new();
                for key in ["name", "schema", "strict", "description"] {
                    if let Some(value) = format.get(key) {
                        json_schema.insert(key.into(), value.clone());
                    }
                }
                chat.insert(
                    "response_format".into(),
                    json!({"type": "json_schema", "json_schema": json_schema}),
                );
            }
            Some("json_object") => {
                chat.insert("response_format".into(), json!({"type": "json_object"}));
            }
            Some("text") | None => {}
            Some(_) => {}
        }
    }
    Value::Object(chat)
}

fn reasoning_lookup_for_body(
    body: &Value,
    store: &std::sync::Mutex<ReasoningStore>,
) -> ReasoningStore {
    let mut ids = HashSet::new();
    if let Some(items) = body.get("input").and_then(Value::as_array) {
        for item in items.iter().filter_map(Value::as_object) {
            for key in ["id", "call_id"] {
                if let Some(id) = item.get(key).and_then(Value::as_str) {
                    ids.insert(id);
                }
            }
        }
    }
    let store = store
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    store.subset(ids)
}

/// 把 Chat 请求里的 `response_format`（json_schema）降级为系统提示词指令，
/// 兼容不支持结构化输出的第三方 API：移除 response_format，并把 JSON Schema
/// 追加到 system 消息中要求模型按 schema 输出 JSON。
/// 指令刻意写得强硬并显式列出必填字段，降低模型漏字段/加解释的概率。
fn degrade_structured_output(chat: &Value) -> Value {
    let mut degraded = chat.clone();
    degraded
        .as_object_mut()
        .map(|map| map.remove("response_format"));
    let schema = chat
        .pointer("/response_format/json_schema/schema")
        .filter(|value| !value.is_null())
        .cloned();
    let mut instruction = String::from(
        "You MUST respond with ONLY a single valid JSON object. Do not wrap it in markdown \
         code fences, do not add any explanation, preamble, or trailing text before or after the \
         JSON object.",
    );
    if let Some(schema) = schema {
        instruction
            .push_str("\nThe JSON object MUST conform exactly to the following JSON Schema:");
        let required = required_fields(&schema);
        if !required.is_empty() {
            instruction.push_str(&format!(
                "\nThe object MUST include every one of these top-level fields: {}.\
                 \nDo not omit any field; use the exact field names and value types defined by the schema.",
                required.join(", ")
            ));
        }
        let schema_text =
            serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());
        instruction.push_str(&format!("\n{schema_text}"));
    }
    let Some(messages) = degraded.get_mut("messages").and_then(Value::as_array_mut) else {
        return degraded;
    };
    if let Some(system) = messages
        .iter_mut()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("system"))
    {
        match system.get("content") {
            Some(Value::String(text)) => {
                system["content"] = Value::String(format!("{text}\n\n{instruction}"));
            }
            Some(Value::Array(parts)) => {
                let mut parts = parts.clone();
                parts.push(json!({ "type": "text", "text": instruction }));
                system["content"] = Value::Array(parts);
            }
            _ => {
                system["content"] = Value::String(instruction.clone());
            }
        }
    } else {
        messages.insert(0, json!({ "role": "system", "content": instruction }));
    }
    degraded
}

/// 从 JSON Schema 提取顶层必填字段。没有 `required` 时所有字段均为可选，
/// 不能为了提示模型而擅自改变 schema 语义。
fn required_fields(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn input_to_messages(body: &Value, store: &ReasoningStore) -> Vec<Value> {
    // 预分配：大致等于输入条目数 + 可能的 system 消息。
    let capacity = body
        .get("input")
        .and_then(Value::as_array)
        .map_or(1, |items| items.len() + 1);
    let mut messages: Vec<Value> = Vec::with_capacity(capacity);
    if let Some(instructions) = body
        .get("instructions")
        .and_then(Value::as_str)
        .filter(|instructions| !instructions.trim().is_empty())
    {
        messages.push(json!({"role": "system", "content": instructions}));
    }
    match body.get("input") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) => {
            messages.push(json!({"role": "user", "content": text}));
        }
        Some(Value::Array(items)) => {
            let mut pending_tool_calls: Vec<Value> = Vec::new();
            let mut pending_reasoning: Option<String> = None;
            for item in items {
                match item {
                    Value::String(text) => {
                        flush_tool_calls(
                            &mut messages,
                            &mut pending_tool_calls,
                            &mut pending_reasoning,
                        );
                        messages.push(json!({"role": "user", "content": text}));
                    }
                    Value::Object(map) => {
                        let item_type =
                            map.get("type").and_then(Value::as_str).unwrap_or("message");
                        match item_type {
                            "function_call" => {
                                pending_tool_calls.push(json!({
                                    "id": map.get("call_id").or_else(|| map.get("id")).and_then(Value::as_str).unwrap_or(""),
                                    "type": "function",
                                    "function": {
                                        "name": map.get("name").and_then(Value::as_str).unwrap_or(""),
                                        "arguments": map.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                                    }
                                }));
                                // 同属上一轮 Chat assistant 消息的 thinking 内容：
                                // 由转换代理记住，必须回传给 DeepSeek 等 API。
                                // 先按条目 id 查，条目 id 被重写时回退到 call_id。
                                if pending_reasoning.is_none() {
                                    let by_item = map
                                        .get("id")
                                        .and_then(Value::as_str)
                                        .and_then(|id| store.get(id));
                                    let by_call = map
                                        .get("call_id")
                                        .and_then(Value::as_str)
                                        .and_then(|id| store.get(id));
                                    if let Some(reasoning) = by_item.or(by_call) {
                                        pending_reasoning = Some(reasoning.to_owned());
                                    }
                                }
                            }
                            "function_call_output" => {
                                flush_tool_calls(
                                    &mut messages,
                                    &mut pending_tool_calls,
                                    &mut pending_reasoning,
                                );
                                messages.push(json!({
                                    "role": "tool",
                                    "tool_call_id": map.get("call_id").and_then(Value::as_str).unwrap_or(""),
                                    "content": output_to_text(map.get("output").unwrap_or(&Value::Null)),
                                }));
                            }
                            "message" | "" => {
                                flush_tool_calls(
                                    &mut messages,
                                    &mut pending_tool_calls,
                                    &mut pending_reasoning,
                                );
                                let role =
                                    map.get("role").and_then(Value::as_str).unwrap_or("user");
                                let role = if role == "developer" { "system" } else { role };
                                let mut message = json!({"role": role, "content": message_content_to_chat(map.get("content").unwrap_or(&Value::Null))});
                                if role == "assistant"
                                    && let Some(reasoning) = map
                                        .get("id")
                                        .and_then(Value::as_str)
                                        .and_then(|id| store.get(id))
                                {
                                    message["reasoning_content"] =
                                        Value::String(reasoning.to_owned());
                                }
                                if let Some(tool_calls) = extract_tool_calls_from_content(
                                    map.get("content").unwrap_or(&Value::Null),
                                ) {
                                    message["tool_calls"] = tool_calls;
                                }
                                messages.push(message);
                            }
                            // reasoning、web_search_call、file 等输入项无法映射到
                            // Chat Completions，直接忽略。
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            flush_tool_calls(
                &mut messages,
                &mut pending_tool_calls,
                &mut pending_reasoning,
            );
        }
        _ => {}
    }
    normalize_system_messages(messages)
}

/// Chat templates commonly accept exactly one system message and require it at
/// index zero. Responses input can contain developer messages later in the
/// history, so merge every system-level instruction before forwarding.
fn normalize_system_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut system_content = String::new();
    let mut regular = Vec::with_capacity(messages.len());
    let mut had_system = false;
    for message in messages {
        if message.get("role").and_then(Value::as_str) == Some("system") {
            had_system = true;
            if let Some(content) = message.get("content").and_then(chat_content_to_text)
                && !content.trim().is_empty()
            {
                if !system_content.is_empty() {
                    system_content.push_str("\n\n");
                }
                system_content.push_str(&content);
            }
        } else {
            regular.push(message);
        }
    }
    if had_system {
        regular.insert(0, json!({ "role": "system", "content": system_content }));
    }
    regular
}

fn flush_tool_calls(
    messages: &mut Vec<Value>,
    pending_tool_calls: &mut Vec<Value>,
    pending_reasoning: &mut Option<String>,
) {
    if pending_tool_calls.is_empty() {
        pending_reasoning.take();
        return;
    }
    if let Some(message) = messages.last_mut()
        && message.get("role").and_then(Value::as_str) == Some("assistant")
        && message.get("tool_calls").is_none()
    {
        message["tool_calls"] = Value::Array(std::mem::take(pending_tool_calls));
        if let Some(reasoning) = pending_reasoning.take() {
            message["reasoning_content"] = Value::String(reasoning);
        }
        return;
    }
    let mut message = json!({
        "role": "assistant",
        "content": "",
        "tool_calls": std::mem::take(pending_tool_calls),
    });
    if let Some(reasoning) = pending_reasoning.take() {
        message["reasoning_content"] = Value::String(reasoning);
    }
    messages.push(message);
}

fn output_to_text(output: &Value) -> String {
    match output {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// 把 Responses 消息的 content（字符串或内容块数组）转成 Chat 的 content。
fn message_content_to_chat(content: &Value) -> Value {
    match content {
        Value::String(text) => Value::String(text.clone()),
        Value::Array(parts) => {
            let mut converted: Vec<Value> = Vec::new();
            for part in parts {
                let Some(map) = part.as_object() else {
                    continue;
                };
                let part_type = map.get("type").and_then(Value::as_str).unwrap_or("text");
                match part_type {
                    "input_text" | "output_text" | "text" => {
                        if let Some(text) = map.get("text").and_then(Value::as_str) {
                            converted.push(json!({"type": "text", "text": text}));
                        }
                    }
                    "input_image" => {
                        converted.push(json!({
                            "type": "image_url",
                            "image_url": {
                                "url": map.get("image_url").and_then(Value::as_str).unwrap_or(""),
                                "detail": map.get("detail").cloned().unwrap_or(Value::String("auto".into())),
                            }
                        }));
                    }
                    "refusal" => {
                        if let Some(text) = map.get("refusal").and_then(Value::as_str) {
                            converted.push(json!({"type": "text", "text": text}));
                        }
                    }
                    // function_call 内容块单独转成 tool_calls，不进入 content。
                    _ => {}
                }
            }
            if converted.len() == 1
                && let Some(text) = converted[0].get("text").and_then(Value::as_str)
            {
                Value::String(text.to_owned())
            } else if converted.is_empty() {
                Value::String(String::new())
            } else {
                Value::Array(converted)
            }
        }
        _ => Value::String(String::new()),
    }
}

/// 旧式 Responses 消息把 function_call 放在 content 内容块里，这里转成
/// Chat 的 tool_calls。
fn extract_tool_calls_from_content(content: &Value) -> Option<Value> {
    let parts = content.as_array()?;
    let mut calls = Vec::new();
    for part in parts {
        let Some(map) = part.as_object() else {
            continue;
        };
        if map.get("type").and_then(Value::as_str) != Some("function_call") {
            continue;
        }
        calls.push(json!({
            "id": map.get("call_id").or_else(|| map.get("id")).and_then(Value::as_str).unwrap_or(""),
            "type": "function",
            "function": {
                "name": map.get("name").and_then(Value::as_str).unwrap_or(""),
                "arguments": map.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
            }
        }));
    }
    (!calls.is_empty()).then_some(Value::Array(calls))
}

/// Responses 工具是扁平结构，Chat 需要嵌套在 function 里。Schema 默认原样
/// 保留；静默删除 strict/additionalProperties 会改变工具契约并可能误删同名属性。
fn convert_tools(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .filter_map(|tool| {
            let map = tool.as_object()?;
            if map.get("type").and_then(Value::as_str) != Some("function") {
                return None;
            }
            let mut function = Map::new();
            if let Some(name) = map.get("name").and_then(Value::as_str) {
                function.insert("name".into(), Value::String(name.into()));
            }
            if let Some(description) = map.get("description").and_then(Value::as_str) {
                function.insert("description".into(), Value::String(description.into()));
            }
            if let Some(parameters) = map.get("parameters").cloned() {
                function.insert("parameters".into(), parameters);
            }
            if let Some(strict) = map.get("strict").filter(|value| !value.is_null()) {
                function.insert("strict".into(), strict.clone());
            }
            Some(json!({"type": "function", "function": function}))
        })
        .collect()
}

fn convert_tool_choice(tool_choice: &Value) -> Value {
    match tool_choice {
        Value::String(value) => Value::String(value.clone()),
        Value::Object(map) if map.get("type").and_then(Value::as_str) == Some("function") => {
            json!({
                "type": "function",
                "function": {
                    "name": map.get("name").and_then(Value::as_str).unwrap_or(""),
                }
            })
        }
        _ => Value::String("auto".into()),
    }
}

/// Third-party Chat APIs sometimes return structured content as an object, a
/// content-part array, or the SDK-style `parsed` field instead of a string.
fn chat_content_to_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| match part {
                    Value::String(text) => Some(text.clone()),
                    Value::Object(map) => map
                        .get("text")
                        .or_else(|| map.get("content"))
                        .and_then(chat_content_to_text),
                    _ => None,
                })
                .collect::<String>();
            (!text.is_empty()).then_some(text)
        }
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .and_then(chat_content_to_text)
            .or_else(|| serde_json::to_string(value).ok()),
        _ => None,
    }
}

fn chat_content_to_text_cow(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(text) => Some(Cow::Borrowed(text)),
        _ => chat_content_to_text(value).map(Cow::Owned),
    }
}

fn message_reasoning_text(message: &Value) -> Option<String> {
    [
        "reasoning_content",
        "reasoning",
        "analysis",
        "reasoning_details",
    ]
    .iter()
    .find_map(|key| message.get(key).and_then(chat_content_to_text))
    .filter(|text| !text.trim().is_empty())
}

fn validate_structured_output(text: &str, schema: Option<&Value>) -> Option<String> {
    let value = serde_json::from_str::<Value>(text).ok()?;
    if !value.is_object() {
        return None;
    }
    if schema.is_some_and(|schema| !jsonschema::is_valid(schema, &value)) {
        return None;
    }
    serde_json::to_string(&value).ok()
}

fn message_output_text(message: &Value, structured_output: bool) -> String {
    if structured_output {
        return ["content", "parsed"]
            .into_iter()
            .filter_map(|key| message.get(key))
            .find_map(|value| match value {
                Value::Object(_) => serde_json::to_string(value).ok(),
                _ => chat_content_to_text(value),
            })
            .filter(|text| !text.trim().is_empty())
            .unwrap_or_default();
    }
    let content = message
        .get("content")
        .and_then(chat_content_to_text)
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            message
                .get("parsed")
                .and_then(chat_content_to_text)
                .filter(|text| !text.trim().is_empty())
        });
    content.unwrap_or_default()
}

// ---------------------------------------------------------------------------
// 非流式响应转换：Chat Completions → Responses API
// ---------------------------------------------------------------------------

#[cfg(test)]
fn chat_to_responses_body(
    chat: &Value,
    response_id: &str,
    model: &str,
    created_at: i64,
    store: &std::sync::Mutex<ReasoningStore>,
    structured_output: bool,
) -> Result<Value, String> {
    chat_to_responses_body_with_schema(
        chat,
        response_id,
        model,
        created_at,
        store,
        structured_output,
        None,
    )
}

fn chat_to_responses_body_with_schema(
    chat: &Value,
    response_id: &str,
    model: &str,
    created_at: i64,
    store: &std::sync::Mutex<ReasoningStore>,
    structured_output: bool,
    structured_schema: Option<&Value>,
) -> Result<Value, String> {
    if let Some(error) = chat.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("上游服务在成功状态响应中返回了错误。");
        return Err(message.to_owned());
    }
    let choice = chat
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| "上游 Chat Completions 响应缺少有效 choices。".to_string())?;
    let message = choice
        .get("message")
        .filter(|value| value.is_object())
        .ok_or_else(|| "上游 Chat Completions 响应缺少有效 assistant message。".to_string())?;
    let response_model = chat
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .unwrap_or(model);
    let mut content_text = message_output_text(message, structured_output);
    let refusal = message
        .get("refusal")
        .and_then(chat_content_to_text)
        .filter(|text| !text.is_empty());
    let has_tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty());
    if message.get("function_call").is_some() {
        return Err("上游返回了旧版 function_call；转换代理要求标准 tool_calls 格式。".into());
    }
    if structured_output && refusal.is_none() {
        content_text = match validate_structured_output(&content_text, structured_schema) {
            Some(content) => content,
            None if has_tool_calls => String::new(),
            None => return Err("上游模型未返回有效的结构化 JSON 对象。".into()),
        };
    }
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let (status, incomplete_reason) = match choice.get("finish_reason").and_then(Value::as_str) {
        Some("stop" | "tool_calls" | "function_call") => ("completed", None),
        None => return Err("上游 Chat Completions 响应缺少 finish_reason。".into()),
        Some("length") => ("incomplete", Some("max_output_tokens")),
        Some("content_filter") => ("incomplete", Some("content_filter")),
        Some(reason) => return Err(format!("上游返回了无法识别的 finish_reason：{reason}")),
    };
    let item_status = if incomplete_reason.is_some() {
        "incomplete"
    } else {
        "completed"
    };
    // 记住上一轮 reasoning_content，供下一轮请求回传（DeepSeek 等要求）。
    // 按输出条目 id 与工具 call_id 双重索引，id 被重写时仍能匹配。
    if let Some(reasoning) = message_reasoning_text(message) {
        if let Ok(mut store) = store.lock() {
            let reasoning = ReasoningStore::bounded_content(&reasoning);
            store.insert_shared(&format!("msg_{suffix}"), reasoning.clone());
            for (index, tool_call) in tool_calls.iter().enumerate() {
                let fc_id = format!("fc_{suffix}_{index}");
                store.insert_shared(&fc_id, reasoning.clone());
                if let Some(call_id) = tool_call.get("id").and_then(Value::as_str) {
                    store.insert_call_alias(call_id, reasoning.clone());
                }
            }
        }
    }
    let mut output = Vec::new();
    if !content_text.is_empty() || refusal.is_some() {
        let mut content = Vec::new();
        if !content_text.is_empty() {
            content.push(json!({"type": "output_text", "text": content_text, "annotations": []}));
        }
        if let Some(refusal) = refusal {
            content.push(json!({"type": "refusal", "refusal": refusal}));
        }
        output.push(json!({
            "id": format!("msg_{suffix}"),
            "type": "message",
            "status": item_status,
            "role": "assistant",
            "content": content,
        }));
    }
    for (index, tool_call) in tool_calls.iter().enumerate() {
        let function = tool_call.get("function").unwrap_or(&Value::Null);
        let call_id = tool_call
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("上游第 {} 个工具调用缺少 id。", index + 1))?;
        let name = function
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("上游第 {} 个工具调用缺少函数名。", index + 1))?;
        let arguments = function
            .get("arguments")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("上游第 {} 个工具调用参数不是字符串。", index + 1))?;
        output.push(json!({
            "id": format!("fc_{suffix}_{index}"),
            "type": "function_call",
            "status": item_status,
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
        }));
    }
    let usage = chat
        .get("usage")
        .filter(|value| value.is_object())
        .map(|usage| {
        let input_tokens = usage
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let output_tokens = usage
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let total_tokens = usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| input_tokens.saturating_add(output_tokens));
        json!({
            "input_tokens": input_tokens,
            "input_tokens_details": {
                "cached_tokens": usage.pointer("/prompt_tokens_details/cached_tokens").and_then(Value::as_u64).unwrap_or(0),
                "cache_write_tokens": usage.pointer("/prompt_tokens_details/cache_write_tokens").and_then(Value::as_u64).unwrap_or(0)
            },
            "output_tokens": output_tokens,
            "output_tokens_details": {
                "reasoning_tokens": usage.pointer("/completion_tokens_details/reasoning_tokens").and_then(Value::as_u64).unwrap_or(0)
            },
            "total_tokens": total_tokens,
        })
        });
    Ok(json!({
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": status,
        "model": response_model,
        "output": output,
        "error": null,
        "incomplete_details": incomplete_reason.map(|reason| json!({"reason": reason})),
        "usage": usage,
        "output_text": content_text,
        "parallel_tool_calls": true,
        "previous_response_id": null,
        "reasoning": {"effort": null, "summary": null},
        "text": {"format": {"type": "text"}},
        "tools": [],
        "tool_choice": "auto",
        "temperature": null,
        "top_p": null,
        "truncation": "disabled",
        "store": true,
        "metadata": {},
        "user": null,
    }))
}

// ---------------------------------------------------------------------------
// 流式响应转换：Chat SSE → Responses SSE 事件
// ---------------------------------------------------------------------------

struct StreamTranslator {
    response_id: String,
    model: String,
    created_at: i64,
    msg_item_id: String,
    msg_output_index: Option<usize>,
    message_started: bool,
    full_text: String,
    text_started: bool,
    text_content_index: Option<usize>,
    refusal_text: String,
    refusal_started: bool,
    refusal_content_index: Option<usize>,
    /// 本响应累积的完整思考内容（reasoning_content），
    /// 结束后按输出条目 id 存入 store 供下一轮回传。
    reasoning_content: String,
    structured_output: bool,
    structured_schema: Option<Value>,
    metadata: ResponseMetadata,
    tool_calls: BTreeMap<usize, ToolCallAcc>,
    next_output_index: usize,
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
    total_tokens: Option<u64>,
    saw_usage: bool,
    finish_reason: Option<String>,
    saw_valid_choice: bool,
    sequence: u32,
    has_failed_event: bool,
}

struct ToolCallAcc {
    id: String,
    name: String,
    arguments: String,
    item_id: String,
    output_index: Option<usize>,
    started: bool,
    emitted_arguments_len: usize,
}

impl StreamTranslator {
    #[cfg(test)]
    fn new(response_id: &str, model: &str, created_at: i64) -> Self {
        Self::new_with_structured_output(response_id, model, created_at, false)
    }

    #[cfg(test)]
    fn new_with_structured_output(
        response_id: &str,
        model: &str,
        created_at: i64,
        structured_output: bool,
    ) -> Self {
        Self::new_with_structured_schema(response_id, model, created_at, structured_output, None)
    }

    #[cfg(test)]
    fn new_with_structured_schema(
        response_id: &str,
        model: &str,
        created_at: i64,
        structured_output: bool,
        structured_schema: Option<Value>,
    ) -> Self {
        Self::new_with_metadata(
            response_id,
            model,
            created_at,
            structured_output,
            structured_schema,
            ResponseMetadata::default(),
        )
    }

    fn new_with_metadata(
        response_id: &str,
        model: &str,
        created_at: i64,
        structured_output: bool,
        structured_schema: Option<Value>,
        metadata: ResponseMetadata,
    ) -> Self {
        Self {
            response_id: response_id.to_owned(),
            model: model.to_owned(),
            created_at,
            msg_item_id: format!("msg_{}", uuid::Uuid::new_v4().simple()),
            msg_output_index: None,
            message_started: false,
            full_text: String::new(),
            text_started: false,
            text_content_index: None,
            refusal_text: String::new(),
            refusal_started: false,
            refusal_content_index: None,
            reasoning_content: String::new(),
            structured_output,
            structured_schema,
            metadata,
            tool_calls: BTreeMap::new(),
            next_output_index: 0,
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: 0,
            cache_write_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: None,
            saw_usage: false,
            finish_reason: None,
            saw_valid_choice: false,
            sequence: 0,
            has_failed_event: false,
        }
    }

    fn stub(&self, event_type: &str, status: &str) -> Value {
        let mut event = json!({
            "type": event_type,
            "response": {
                "id": self.response_id,
                "object": "response",
                "created_at": self.created_at,
                "status": status,
                "model": self.model,
                "output": [],
                "usage": null,
                "error": null,
                "incomplete_details": null,
                "instructions": null,
                "parallel_tool_calls": true,
                "tool_choice": "auto",
                "tools": [],
            }
        });
        self.metadata.apply(&mut event["response"]);
        event
    }

    /// 处理一个上游流式分片。返回 `Some(错误信息)` 表示上游在流中报告了错误，
    /// 调用方应停止继续读取并结束本次响应。
    fn push_chunk(&mut self, chunk: &Value, out: &mut Vec<PendingEvent>) -> Option<String> {
        if let Some(error) = chunk.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("上游服务在流式响应中报告了错误。");
            self.fail(out, message);
            return Some(message.to_owned());
        }
        if let Some(model) = chunk
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
            .filter(|model| *model != self.model)
        {
            self.model.clear();
            self.model.push_str(model);
        }
        if let Some(usage) = chunk.get("usage") {
            self.saw_usage |= ["prompt_tokens", "completion_tokens", "total_tokens"]
                .into_iter()
                .any(|key| usage.get(key).and_then(Value::as_u64).is_some());
            self.input_tokens = usage
                .get("prompt_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.input_tokens);
            self.output_tokens = usage
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.output_tokens);
            self.cached_tokens = usage
                .pointer("/prompt_tokens_details/cached_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.cached_tokens);
            self.cache_write_tokens = usage
                .pointer("/prompt_tokens_details/cache_write_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.cache_write_tokens);
            self.reasoning_tokens = usage
                .pointer("/completion_tokens_details/reasoning_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.reasoning_tokens);
            self.total_tokens = usage
                .get("total_tokens")
                .and_then(Value::as_u64)
                .or(self.total_tokens);
        }
        let Some(choices) = chunk.get("choices").and_then(Value::as_array) else {
            if chunk.get("usage").is_some() {
                return None;
            }
            let message = "上游流式响应缺少 choices 数组。";
            self.fail(out, message);
            return Some(message.into());
        };
        let Some(choice) = choices.first() else {
            // include_usage 的末尾分片按协议 choices 为空。
            return None;
        };
        self.saw_valid_choice = true;
        let Some(delta) = choice.get("delta").filter(|value| value.is_object()) else {
            let message = "上游流式响应包含无效 delta。";
            self.fail(out, message);
            return Some(message.into());
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = Some(reason.to_owned());
        }
        // reasoning 只在代理内部保存并用于工具调用续传。向 Responses 暴露它必须
        // 生成完整 reasoning item 生命周期，不能发送没有 added/done 的幽灵 delta。
        if let Some(reasoning) = message_reasoning_text(delta) {
            self.reasoning_content.push_str(&reasoning);
        }
        if let Some(text) = delta
            .get("content")
            .and_then(chat_content_to_text_cow)
            .or_else(|| delta.get("parsed").and_then(chat_content_to_text_cow))
            .filter(|text| !text.is_empty())
        {
            self.full_text.push_str(&text);
            if !self.structured_output {
                self.start_text(out);
                let sequence = self.sequence;
                self.sequence = self.sequence.saturating_add(1);
                let output_index = self.msg_output_index.unwrap_or(0);
                out.push(text_delta_event(
                    "response.output_text.delta",
                    &self.msg_item_id,
                    output_index,
                    self.text_content_index.unwrap_or(0),
                    sequence,
                    &text,
                ));
            }
        }
        if let Some(refusal) = delta
            .get("refusal")
            .and_then(chat_content_to_text_cow)
            .filter(|text| !text.is_empty())
        {
            self.start_refusal(out);
            self.refusal_text.push_str(&refusal);
            let content_index = self.refusal_content_index.unwrap_or(0);
            out.push(sequenced_sse_event(
                &mut self.sequence,
                "response.refusal.delta",
                json!({
                    "type": "response.refusal.delta",
                    "item_id": self.msg_item_id,
                    "output_index": self.msg_output_index.unwrap_or(0),
                    "content_index": content_index,
                    "delta": refusal,
                }),
            ));
        }
        if delta.get("function_call").is_some() {
            let message = "上游返回了旧版 function_call；转换代理要求标准 tool_calls 格式。";
            self.fail(out, message);
            return Some(message.into());
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for (position, tool_call) in tool_calls.iter().enumerate() {
                let index = tool_call
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|index| index as usize)
                    .unwrap_or(position);
                let function = tool_call.get("function").unwrap_or(&Value::Null);
                let incoming_id = tool_call.get("id").and_then(Value::as_str);
                let incoming_name = function.get("name").and_then(Value::as_str);
                if let Some(existing) = self.tool_calls.get(&index)
                    && (incoming_id.is_some_and(|id| !existing.id.is_empty() && existing.id != id)
                        || incoming_name
                            .is_some_and(|name| !existing.name.is_empty() && existing.name != name))
                {
                    let message = format!("上游工具调用索引 {index} 的 id 或函数名发生冲突。");
                    self.fail(out, &message);
                    return Some(message);
                }
                let arguments_delta = match function.get("arguments") {
                    None | Some(Value::Null) => "",
                    Some(Value::String(arguments)) => arguments.as_str(),
                    Some(_) => {
                        let message =
                            format!("上游工具调用索引 {index} 的 function.arguments 不是字符串。");
                        self.fail(out, &message);
                        return Some(message);
                    }
                };
                let acc = self.tool_calls.entry(index).or_insert_with(|| ToolCallAcc {
                    id: String::new(),
                    name: String::new(),
                    arguments: String::new(),
                    item_id: format!("fc_{}", uuid::Uuid::new_v4().simple()),
                    output_index: None,
                    started: false,
                    emitted_arguments_len: 0,
                });
                if let Some(id) = incoming_id {
                    acc.id = id.to_owned();
                }
                if let Some(name) = incoming_name {
                    acc.name = name.to_owned();
                }
                acc.arguments.push_str(arguments_delta);
                // Chat 服务经常把 id/name 拆到不同分片。只有身份完整后才开启 item，
                // 避免 added 事件永久携带空函数名或空 call_id。
                if !acc.started && !acc.id.is_empty() && !acc.name.is_empty() {
                    acc.started = true;
                    if acc.output_index.is_none() {
                        let index = self.next_output_index;
                        self.next_output_index += 1;
                        acc.output_index = Some(index);
                    }
                    let output_index = acc.output_index.unwrap_or(0);
                    out.push(sequenced_sse_event(
                        &mut self.sequence,
                        "response.output_item.added",
                        json!({
                            "type": "response.output_item.added",
                            "output_index": output_index,
                            "item": {
                                "id": acc.item_id,
                                "type": "function_call",
                                "status": "in_progress",
                                "call_id": acc.id,
                                "name": acc.name,
                                "arguments": "",
                            },
                        }),
                    ));
                }
                if acc.started && acc.emitted_arguments_len < acc.arguments.len() {
                    let arguments_delta = &acc.arguments[acc.emitted_arguments_len..];
                    let output_index = acc.output_index.unwrap_or(0);
                    let sequence = self.sequence;
                    self.sequence = self.sequence.saturating_add(1);
                    out.push(arguments_delta_event(
                        &acc.item_id,
                        output_index,
                        sequence,
                        arguments_delta,
                    ));
                    acc.emitted_arguments_len = acc.arguments.len();
                }
            }
        }
        None
    }

    fn claim_output_index(&mut self) -> usize {
        let index = self.next_output_index;
        self.next_output_index += 1;
        index
    }

    fn start_text(&mut self, out: &mut Vec<PendingEvent>) {
        if self.text_started {
            return;
        }
        self.text_started = true;
        self.start_message(out);
        let output_index = self.msg_output_index.unwrap_or(0);
        let content_index = usize::from(self.refusal_started);
        self.text_content_index = Some(content_index);
        out.push(sequenced_sse_event(
            &mut self.sequence,
            "response.content_part.added",
            json!({
                "type": "response.content_part.added",
                "item_id": self.msg_item_id,
                "output_index": output_index,
                "content_index": content_index,
                "part": {"type": "output_text", "text": "", "annotations": []},
            }),
        ));
    }

    fn start_message(&mut self, out: &mut Vec<PendingEvent>) {
        if self.message_started {
            return;
        }
        self.message_started = true;
        let output_index = self.claim_output_index();
        self.msg_output_index = Some(output_index);
        out.push(sequenced_sse_event(
            &mut self.sequence,
            "response.output_item.added",
            json!({
                "type": "response.output_item.added",
                "output_index": output_index,
                "item": {
                    "id": self.msg_item_id,
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [],
                },
            }),
        ));
    }

    fn start_refusal(&mut self, out: &mut Vec<PendingEvent>) {
        if self.refusal_started {
            return;
        }
        self.refusal_started = true;
        self.start_message(out);
        let content_index = usize::from(self.text_started);
        self.refusal_content_index = Some(content_index);
        out.push(sequenced_sse_event(
            &mut self.sequence,
            "response.content_part.added",
            json!({
                "type": "response.content_part.added",
                "item_id": self.msg_item_id,
                "output_index": self.msg_output_index.unwrap_or(0),
                "content_index": content_index,
                "part": {"type": "refusal", "refusal": ""},
            }),
        ));
    }

    fn fail(&mut self, out: &mut Vec<PendingEvent>, message: &str) {
        if self.has_failed_event {
            return;
        }
        self.has_failed_event = true;
        let mut response = self.stub("response.failed", "failed");
        response["response"]["error"] = json!({"code": "server_error", "message": message});
        response["response"]["incomplete_details"] = Value::Null;
        out.push(sequenced_sse_event(
            &mut self.sequence,
            "response.failed",
            response,
        ));
    }

    fn finish(
        &mut self,
        out: &mut Vec<PendingEvent>,
        store: &std::sync::Mutex<ReasoningStore>,
    ) -> bool {
        if !self.saw_valid_choice {
            self.fail(out, "上游流式响应没有包含任何有效 choice。");
            return false;
        }
        let incomplete_reason = match self.finish_reason.as_deref() {
            Some("stop" | "tool_calls" | "function_call") => None,
            None => {
                self.fail(out, "上游 Chat Completions 流未提供终止 finish_reason。");
                return false;
            }
            Some("length") => Some("max_output_tokens"),
            Some("content_filter") => Some("content_filter"),
            Some(reason) => {
                self.fail(
                    out,
                    &format!("上游返回了无法识别的 finish_reason：{reason}"),
                );
                return false;
            }
        };
        if let Some((index, _)) = self
            .tool_calls
            .iter()
            .find(|(_, call)| call.id.is_empty() || call.name.is_empty())
        {
            self.fail(
                out,
                &format!("上游第 {} 个工具调用缺少 id 或函数名。", index + 1),
            );
            return false;
        }
        // 把本响应的完整思考内容按输出条目 id 与工具 call_id 记住，
        // 供下一轮请求回传（id 被重写时 call_id 仍能匹配）。
        if !self.reasoning_content.trim().is_empty()
            && let Ok(mut store) = store.lock()
        {
            let reasoning = ReasoningStore::bounded_content(&self.reasoning_content);
            store.insert_shared(&self.msg_item_id, reasoning.clone());
            for acc in self.tool_calls.values() {
                store.insert_shared(&acc.item_id, reasoning.clone());
                if !acc.id.is_empty() {
                    store.insert_call_alias(&acc.id, reasoning.clone());
                }
            }
        }
        if self.structured_output && !self.text_started && !self.refusal_started {
            let has_text = !self.full_text.trim().is_empty();
            let text = if has_text {
                validate_structured_output(&self.full_text, self.structured_schema.as_ref())
            } else {
                None
            };
            if text.is_none() && self.tool_calls.is_empty() {
                self.fail(out, "上游模型未返回有效的结构化 JSON 对象。");
                return false;
            }
            if let Some(text) = text {
                self.start_text(out);
                self.full_text = text;
                let sequence = self.sequence;
                self.sequence = self.sequence.saturating_add(1);
                out.push(text_delta_event(
                    "response.output_text.delta",
                    &self.msg_item_id,
                    self.msg_output_index.unwrap_or(0),
                    self.text_content_index.unwrap_or(0),
                    sequence,
                    &self.full_text,
                ));
            } else {
                self.full_text.clear();
            }
        }
        let mut output = Vec::new();
        let item_status = if incomplete_reason.is_some() {
            "incomplete"
        } else {
            "completed"
        };
        if self.message_started {
            let output_index = self.msg_output_index.unwrap_or(0);
            let mut content = Vec::new();
            if self.text_started {
                let content_index = self.text_content_index.unwrap_or(0);
                let part =
                    json!({"type": "output_text", "text": self.full_text, "annotations": []});
                out.push(sequenced_sse_event(
                    &mut self.sequence,
                    "response.output_text.done",
                    json!({
                        "type": "response.output_text.done",
                        "item_id": self.msg_item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "text": self.full_text,
                        "logprobs": [],
                    }),
                ));
                out.push(sequenced_sse_event(
                    &mut self.sequence,
                    "response.content_part.done",
                    json!({
                        "type": "response.content_part.done",
                        "item_id": self.msg_item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "part": part,
                    }),
                ));
                content.push((content_index, part));
            }
            if self.refusal_started {
                let content_index = self.refusal_content_index.unwrap_or(0);
                let part = json!({"type": "refusal", "refusal": self.refusal_text});
                out.push(sequenced_sse_event(
                    &mut self.sequence,
                    "response.refusal.done",
                    json!({
                        "type": "response.refusal.done",
                        "item_id": self.msg_item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "refusal": self.refusal_text,
                    }),
                ));
                out.push(sequenced_sse_event(
                    &mut self.sequence,
                    "response.content_part.done",
                    json!({
                        "type": "response.content_part.done",
                        "item_id": self.msg_item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "part": part,
                    }),
                ));
                content.push((content_index, part));
            }
            content.sort_by_key(|(index, _)| *index);
            let content = content
                .into_iter()
                .map(|(_, part)| part)
                .collect::<Vec<_>>();
            let item = json!({
                "id": self.msg_item_id,
                "type": "message",
                "status": item_status,
                "role": "assistant",
                "content": content,
            });
            out.push(sequenced_sse_event(
                &mut self.sequence,
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": output_index,
                    "item": item,
                }),
            ));
            output.push((output_index, item));
        }
        let mut tools = self
            .tool_calls
            .values()
            .filter_map(|acc| acc.output_index.map(|output_index| (output_index, acc)))
            .collect::<Vec<_>>();
        tools.sort_by_key(|(index, _)| *index);
        for (output_index, acc) in tools {
            if incomplete_reason.is_none() {
                out.push(sequenced_sse_event(
                    &mut self.sequence,
                    "response.function_call_arguments.done",
                    json!({
                        "type": "response.function_call_arguments.done",
                        "item_id": acc.item_id,
                        "output_index": output_index,
                        "arguments": acc.arguments,
                        "name": acc.name,
                    }),
                ));
            }
            let item = json!({
                "id": acc.item_id,
                "type": "function_call",
                "status": item_status,
                "call_id": acc.id,
                "name": acc.name,
                "arguments": acc.arguments,
            });
            out.push(sequenced_sse_event(
                &mut self.sequence,
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": output_index,
                    "item": item,
                }),
            ));
            output.push((output_index, item));
        }
        output.sort_by_key(|(index, _)| *index);
        let output = output.into_iter().map(|(_, item)| item).collect::<Vec<_>>();
        let status = if incomplete_reason.is_some() {
            "incomplete"
        } else {
            "completed"
        };
        let event_type = if incomplete_reason.is_some() {
            "response.incomplete"
        } else {
            "response.completed"
        };
        let usage = self.saw_usage.then(|| {
            json!({
            "input_tokens": self.input_tokens,
            "input_tokens_details": {
                "cached_tokens": self.cached_tokens,
                "cache_write_tokens": self.cache_write_tokens
            },
                "output_tokens": self.output_tokens,
                "output_tokens_details": {"reasoning_tokens": self.reasoning_tokens},
            "total_tokens": self.total_tokens.unwrap_or(self.input_tokens.saturating_add(self.output_tokens)),
            })
        });
        let mut response = json!({
            "type": event_type,
            "response": {
                "id": self.response_id,
                "object": "response",
                "created_at": self.created_at,
                "status": status,
                "model": self.model,
                "output": output,
                "usage": usage,
                "parallel_tool_calls": true,
                "tool_choice": "auto",
                "tools": [],
                "instructions": null,
            }
        });
        response["response"]["error"] = Value::Null;
        self.metadata.apply(&mut response["response"]);
        response["response"]["incomplete_details"] = incomplete_reason
            .map(|reason| json!({"reason": reason}))
            .unwrap_or(Value::Null);
        out.push(sequenced_sse_event(
            &mut self.sequence,
            event_type,
            response,
        ));
        true
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn provider(id: &str, base_url: &str) -> ProviderProfile {
        ProviderProfile {
            id: id.into(),
            name: id.into(),
            base_url: base_url.into(),
            headers: Default::default(),
            timeout_secs: 30,
            enabled: true,
            active: false,
            model: String::new(),

            model_context_windows: Default::default(),
            context_window_override: None,
            available_models: Default::default(),
            selected_models: None,
            custom_models: Default::default(),
            models_dev_meta: Default::default(),
            api_type: ProviderApiType::Chat,
            api_key: Some("secret".into()),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;

        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = stream.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            assert!(request.len() <= MAX_REQUEST_BODY_BYTES);

            let Some(header_end) = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            });
            if content_length.is_none_or(|length| request.len() >= header_end + length) {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    #[test]
    fn upstream_endpoint_respects_versioned_roots() {
        assert_eq!(
            crate::provider_http::endpoint_for("https://api.deepseek.com", "chat/completions"),
            "https://api.deepseek.com/v1/chat/completions"
        );
        assert_eq!(
            crate::provider_http::endpoint_for("https://api.deepseek.com/v1", "chat/completions"),
            "https://api.deepseek.com/v1/chat/completions"
        );
        assert_eq!(
            crate::provider_http::endpoint_for("https://gateway.test/openai/v2", "models"),
            "https://gateway.test/openai/v2/models"
        );
    }

    #[test]
    fn truncate_limits_characters_and_handles_multibyte() {
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("abcde", 3), "abc…");
        // 多字节字符按字符截断，不会切坏 UTF-8。
        assert_eq!(truncate("a你b", 2), "a你…");
        assert_eq!(truncate("", 5), "");
        assert_eq!(truncate("abcdef", 0), "…");
    }

    fn empty_reasoning_store() -> std::sync::Mutex<ReasoningStore> {
        std::sync::Mutex::new(ReasoningStore::default())
    }

    #[test]
    fn string_input_and_instructions_become_system_and_user_messages() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "deepseek-chat",
                "instructions": "你是翻译助手",
                "input": "你好",
                "stream": true,
            }),
            &empty_reasoning_store(),
        );
        let messages = chat["messages"].as_array().unwrap();
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "你是翻译助手");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"], "你好");
        assert_eq!(chat["model"], "deepseek-chat");
        assert_eq!(chat["stream"], true);
    }

    #[test]
    fn developer_messages_are_merged_into_the_leading_system_message() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "strict-template-model",
                "instructions": "Base instructions",
                "input": [
                    {"type": "message", "role": "user", "content": "first"},
                    {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "Late developer instruction"}]},
                    {"type": "message", "role": "assistant", "content": "reply"},
                    {"type": "message", "role": "system", "content": "Additional system instruction"},
                    {"type": "message", "role": "user", "content": "second"}
                ]
            }),
            &empty_reasoning_store(),
        );

        let messages = chat["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(
            messages[0]["content"],
            "Base instructions\n\nLate developer instruction\n\nAdditional system instruction"
        );
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(
            messages
                .iter()
                .filter(|message| message["role"] == "system")
                .count(),
            1
        );
    }

    #[test]
    fn multi_turn_tool_conversation_is_reordered() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "deepseek-chat",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "查天气"}]},
                    {"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\":\"北京\"}"},
                    {"type": "function_call_output", "call_id": "call_1", "output": "晴 25℃"},
                    {"type": "message", "role": "user", "content": "然后呢？"}
                ]
            }),
            &empty_reasoning_store(),
        );
        let messages = chat["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "查天气");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "");
        assert_eq!(messages[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_1");
        assert_eq!(messages[2]["content"], "晴 25℃");
        assert_eq!(messages[3]["role"], "user");
    }

    #[test]
    fn json_schema_format_is_converted_to_response_format() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "deepseek-v4-pro",
                "input": "hi",
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "review",
                        "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
                    }
                }
            }),
            &empty_reasoning_store(),
        );
        let format = &chat["response_format"];
        assert_eq!(format["type"], "json_schema");
        assert_eq!(format["json_schema"]["name"], "review");
        assert_eq!(format["json_schema"]["schema"]["type"], "object");
    }

    #[test]
    fn degrade_structured_output_moves_schema_into_system_prompt() {
        let chat = json!({
            "model": "deepseek-v4-pro",
            "messages": [
                {"role": "system", "content": "You are Codex."},
                {"role": "user", "content": "hi"}
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "review",
                    "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
                }
            }
        });
        let degraded = degrade_structured_output(&chat);
        assert!(degraded.get("response_format").is_none());
        let system = &degraded["messages"][0];
        assert_eq!(system["role"], "system");
        let content = system["content"].as_str().unwrap();
        assert!(content.contains("You are Codex."));
        assert!(content.contains("JSON Schema"));
        // schema 没有 required 时不能把可选属性擅自改成必填。
        assert!(!content.contains("top-level fields:"));
        assert!(content.contains("ONLY a single valid JSON object"));
        // 用户消息保持原样。
        assert_eq!(degraded["messages"][1]["role"], "user");
        assert_eq!(degraded["messages"][1]["content"], "hi");
    }

    #[test]
    fn degrade_instruction_lists_schema_required_fields() {
        let chat = json!({
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "hi"}],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "review",
                    "schema": {
                        "type": "object",
                        "required": ["outcome", "message"],
                        "properties": {
                            "outcome": {"type": "string", "enum": ["approved", "rejected"]},
                            "message": {"type": "string"}
                        },
                        "additionalProperties": false
                    }
                }
            }
        });
        let degraded = degrade_structured_output(&chat);
        let system = &degraded["messages"][0];
        let content = system["content"].as_str().unwrap();
        // 必填字段（含 outcome）被显式列出，且强调不得遗漏。
        assert!(content.contains("top-level fields: outcome, message"));
        assert!(content.contains("Do not omit any field"));
        assert!(content.contains("\"outcome\""));
        assert!(content.contains("markdown code fences"));
    }

    #[test]
    fn degrade_structured_output_inserts_system_message_when_missing() {
        let chat = json!({
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "hi"}],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "review",
                    "schema": {"type": "object"}
                }
            }
        });
        let degraded = degrade_structured_output(&chat);
        assert!(degraded.get("response_format").is_none());
        assert_eq!(degraded["messages"][0]["role"], "system");
        assert!(
            degraded["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains("JSON Schema")
        );
        assert_eq!(degraded["messages"][1]["role"], "user");
    }

    #[test]
    fn degrade_json_object_still_adds_json_only_instruction() {
        let degraded = degrade_structured_output(&json!({
            "messages": [{"role": "user", "content": "hi"}],
            "response_format": {"type": "json_object"}
        }));
        assert!(degraded.get("response_format").is_none());
        assert!(
            degraded["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains("ONLY a single valid JSON object")
        );
    }

    #[test]
    fn tools_and_tool_choice_are_converted() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "gpt-4o",
                "input": "hi",
                "tools": [
                    {"type": "function", "name": "f", "description": "desc", "parameters": {"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": false}, "strict": true}
                ],
                "tool_choice": {"type": "function", "name": "f"},
                "max_output_tokens": 128,
                "reasoning": {"effort": "low"},
                "temperature": 0.3,
            }),
            &empty_reasoning_store(),
        );
        let tools = chat["tools"].as_array().unwrap();
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "f");
        assert_eq!(tools[0]["function"]["description"], "desc");
        assert_eq!(
            tools[0]["function"]["parameters"]["additionalProperties"],
            false
        );
        assert_eq!(tools[0]["function"]["strict"], true);
        assert_eq!(chat["tool_choice"]["type"], "function");
        assert_eq!(chat["tool_choice"]["function"]["name"], "f");
        assert_eq!(chat["max_completion_tokens"], 128);
        assert_eq!(chat["reasoning_effort"], "low");
        assert_eq!(chat["temperature"], 0.3);
    }

    #[test]
    fn optional_builtin_tools_are_filtered_while_functions_are_preserved() {
        let request = json!({
            "model": "chat-model",
            "input": "hi",
            "tool_choice": "auto",
            "tools": [
                {"type": "web_search_preview"},
                {"type": "function", "name": "read_file", "parameters": {"type": "object"}}
            ]
        });
        assert!(validate_supported_responses_request(&request).is_ok());

        let chat = responses_to_chat_body(&request, &empty_reasoning_store());
        let tools = chat["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "read_file");

        assert!(
            validate_supported_responses_request(&json!({
                "input": "hi",
                "tool_choice": "required",
                "tools": [{"type": "web_search_preview"}]
            }))
            .is_err()
        );
        assert!(
            validate_supported_responses_request(&json!({
                "input": "hi",
                "tool_choice": {"type": "web_search_preview"},
                "tools": [{"type": "web_search_preview"}]
            }))
            .is_err()
        );
    }

    #[test]
    fn unsupported_state_and_input_items_are_rejected_instead_of_dropped() {
        assert!(
            validate_supported_responses_request(&json!({
                "input": [{"type": "file_search_call", "id": "search_1"}]
            }))
            .is_err()
        );
        assert!(
            validate_supported_responses_request(&json!({
                "input": "hi",
                "text": {"format": {"type": "json_schema", "schema": {"type": "invalid"}}}
            }))
            .is_err()
        );
    }

    #[test]
    fn image_input_is_preserved() {
        let chat = responses_to_chat_body(
            &json!({
                "model": "gpt-4o",
                "input": [{"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "这是什么"},
                    {"type": "input_image", "image_url": "data:image/png;base64,xxx", "detail": "high"}
                ]}]
            }),
            &empty_reasoning_store(),
        );
        let content = chat["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[1]["type"], "image_url");
        assert_eq!(content[1]["image_url"]["url"], "data:image/png;base64,xxx");
        assert_eq!(content[1]["image_url"]["detail"], "high");
    }

    #[test]
    fn non_streaming_response_is_translated() {
        let translated = chat_to_responses_body(
            &json!({
                "id": "chatcmpl-1",
                "created": 1700000000,
                "model": "deepseek-chat",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "你好！",
                        "tool_calls": [{
                            "id": "call_x",
                            "type": "function",
                            "function": {"name": "get_weather", "arguments": "{\"city\":\"北京\"}"}
                        }]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            }),
            "resp_test",
            "deepseek-chat",
            1700000000,
            &empty_reasoning_store(),
            false,
        )
        .unwrap();
        assert_eq!(translated["object"], "response");
        assert_eq!(translated["status"], "completed");
        assert_eq!(translated["id"], "resp_test");
        assert_eq!(translated["created_at"], 1700000000);
        let output = translated["output"].as_array().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["type"], "message");
        assert_eq!(output[0]["content"][0]["text"], "你好！");
        assert_eq!(output[1]["type"], "function_call");
        assert_eq!(output[1]["call_id"], "call_x");
        assert_eq!(output[1]["name"], "get_weather");
        assert_eq!(output[1]["arguments"], "{\"city\":\"北京\"}");
        assert_eq!(translated["usage"]["input_tokens"], 10);
        assert_eq!(translated["usage"]["output_tokens"], 5);
        assert_eq!(translated["usage"]["total_tokens"], 15);
        assert_eq!(translated["output_text"], "你好！");
    }

    fn events_to_json(events: Vec<PendingEvent>) -> Vec<Value> {
        events
            .into_iter()
            .map(|event| {
                serde_json::from_str(&event.data).unwrap_or_else(|error| {
                    panic!(
                        "{} event contains invalid JSON ({error}): {}",
                        event.event_type, event.data
                    )
                })
            })
            .collect()
    }

    #[test]
    fn streaming_translator_emits_spec_events() {
        let mut translator = StreamTranslator::new("resp_x", "deepseek-chat", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"role": "assistant", "content": "你"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"content": "好"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "f", "arguments": "{\"a\":"}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"arguments": "1}"}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}], "usage": {"prompt_tokens": 7, "completion_tokens": 9}}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        let types = values
            .iter()
            .map(|value| value["type"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_item.added",
                "response.function_call_arguments.delta",
                "response.function_call_arguments.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.function_call_arguments.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        let message_added = &values[0];
        assert_eq!(message_added["output_index"], 0);
        assert_eq!(message_added["item"]["type"], "message");
        let tool_added = &values[4];
        assert_eq!(tool_added["output_index"], 1);
        assert_eq!(tool_added["item"]["type"], "function_call");
        assert_eq!(tool_added["item"]["call_id"], "call_1");
        let completed = values.last().unwrap();
        assert_eq!(completed["response"]["status"], "completed");
        assert_eq!(completed["response"]["usage"]["input_tokens"], 7);
        assert_eq!(completed["response"]["usage"]["output_tokens"], 9);
        let output = completed["response"]["output"].as_array().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["type"], "message");
        assert_eq!(output[0]["content"][0]["text"], "你好");
        assert_eq!(output[1]["type"], "function_call");
        assert_eq!(output[1]["arguments"], "{\"a\":1}");
    }

    #[test]
    fn structured_non_streaming_response_uses_parsed_object() {
        let translated = chat_to_responses_body(
            &json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "parsed": {"outcome": "allow"}
                    },
                    "finish_reason": "stop"
                }]
            }),
            "resp_guardian",
            "third-party-model",
            1,
            &empty_reasoning_store(),
            true,
        )
        .unwrap();

        assert_eq!(
            translated["output"][0]["content"][0]["text"],
            r#"{"outcome":"allow"}"#
        );

        let with_text_property = chat_to_responses_body(
            &json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "parsed": {"text": "keep", "ok": true}
                    },
                    "finish_reason": "stop"
                }]
            }),
            "resp_text_property",
            "model",
            1,
            &empty_reasoning_store(),
            true,
        )
        .unwrap();
        assert_eq!(
            with_text_property["output"][0]["content"][0]["text"],
            r#"{"ok":true,"text":"keep"}"#
        );
    }

    #[test]
    fn invalid_structured_output_fails_without_streaming_prose() {
        let non_streaming = chat_to_responses_body(
            &json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "not json"},
                    "finish_reason": "stop"
                }]
            }),
            "resp_invalid",
            "model",
            1,
            &empty_reasoning_store(),
            true,
        );
        assert!(non_streaming.is_err());

        let mut translator =
            StreamTranslator::new_with_structured_output("resp_invalid", "model", 1, true);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"content": "not json"}, "finish_reason": "stop"}]}),
            &mut events,
        );
        assert!(events.is_empty(), "unvalidated prose must remain buffered");
        assert!(!translator.finish(&mut events, &empty_reasoning_store()));
        let values = events_to_json(events);
        assert_eq!(values.last().unwrap()["type"], "response.failed");
        assert!(!values.iter().any(|event| {
            event["type"] == "response.output_text.delta" || event["type"] == "response.completed"
        }));
    }

    #[test]
    fn structured_output_is_validated_against_the_requested_schema() {
        let completion = json!({
            "choices": [{
                "message": {"role": "assistant", "content": "{\"ok\":\"yes\"}"},
                "finish_reason": "stop"
            }]
        });
        let schema = json!({
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": false
        });

        let result = chat_to_responses_body_with_schema(
            &completion,
            "resp_schema",
            "model",
            1,
            &empty_reasoning_store(),
            true,
            Some(&schema),
        );

        assert!(result.is_err());
    }

    #[test]
    fn structured_tool_call_drops_unvalidated_commentary_in_both_modes() {
        let completion = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "I will call a tool",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "f", "arguments": "{}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        });
        let response = chat_to_responses_body(
            &completion,
            "resp_tool",
            "model",
            1,
            &empty_reasoning_store(),
            true,
        )
        .unwrap();
        assert_eq!(response["output"].as_array().unwrap().len(), 1);
        assert_eq!(response["output"][0]["type"], "function_call");

        let mut translator =
            StreamTranslator::new_with_structured_output("resp_tool", "model", 1, true);
        let mut events = Vec::new();
        translator.push_chunk(&completion_to_chunk(&completion), &mut events);
        translator.finish(&mut events, &empty_reasoning_store());
        let terminal = events_to_json(events).pop().unwrap();
        assert_eq!(terminal["response"]["output"].as_array().unwrap().len(), 1);
        assert_eq!(terminal["response"]["output"][0]["type"], "function_call");
    }

    #[test]
    fn structured_stream_never_exposes_reasoning_as_output() {
        let mut translator = StreamTranslator::new_with_structured_output(
            "resp_guardian",
            "third-party-model",
            1,
            true,
        );
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"reasoning_content": "Decision:\n```json\n{\"outcome\":\"allow\"}\n```"}, "finish_reason": "stop"}]}),
            &mut events,
        );
        assert!(!translator.finish(&mut events, &empty_reasoning_store()));

        let values = events_to_json(events);
        assert_eq!(values.last().unwrap()["type"], "response.failed");
        assert!(
            !values
                .iter()
                .any(|value| value["type"] == "response.output_text.delta")
        );
    }

    #[test]
    fn ordinary_stream_does_not_expose_reasoning_as_output() {
        let mut translator = StreamTranslator::new("resp_reasoning", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"reasoning_content": "{\"private\":true}"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "stop"}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        let completed = values.last().unwrap();
        assert_eq!(completed["response"]["output"], json!([]));
    }

    #[test]
    fn finish_reason_length_is_incomplete_in_both_response_modes() {
        let completion = json!({
            "choices": [{
                "message": {"role": "assistant", "content": "partial"},
                "finish_reason": "length"
            }]
        });
        let response = chat_to_responses_body(
            &completion,
            "resp_length",
            "model",
            1,
            &empty_reasoning_store(),
            false,
        )
        .unwrap();
        assert_eq!(response["status"], "incomplete");
        assert_eq!(
            response["incomplete_details"]["reason"],
            "max_output_tokens"
        );

        let mut translator = StreamTranslator::new("resp_length", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(&completion_to_chunk(&completion), &mut events);
        assert!(translator.finish(&mut events, &empty_reasoning_store()));
        let values = events_to_json(events);
        let terminal = values.last().unwrap();
        assert_eq!(terminal["type"], "response.incomplete");
        assert_eq!(
            terminal["response"]["incomplete_details"]["reason"],
            "max_output_tokens"
        );
    }

    #[test]
    fn non_streaming_success_envelope_with_error_or_no_choices_is_rejected() {
        for response in [
            json!({"error": {"message": "bad model"}}),
            json!({}),
            json!({"choices": []}),
        ] {
            assert!(
                chat_to_responses_body(
                    &response,
                    "resp_bad",
                    "model",
                    1,
                    &empty_reasoning_store(),
                    false,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn response_events_have_strictly_increasing_sequence_numbers() {
        let mut translator = StreamTranslator::new("resp_sequence", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"content": "ok", "tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "f", "arguments": "{}"}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        for (expected, event) in events_to_json(events).iter().enumerate() {
            assert_eq!(event["sequence_number"], expected as u64);
        }
    }

    #[test]
    fn refusal_is_preserved_as_a_typed_content_part() {
        let mut translator =
            StreamTranslator::new_with_structured_output("resp_refusal", "model", 1, true);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"refusal": "not allowed"}, "finish_reason": "content_filter"}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        assert!(values.iter().any(|event| {
            event["type"] == "response.refusal.delta" && event["delta"] == "not allowed"
        }));
        let terminal = values.last().unwrap();
        assert_eq!(terminal["type"], "response.incomplete");
        assert_eq!(
            terminal["response"]["output"][0]["content"][0]["type"],
            "refusal"
        );
    }

    #[test]
    fn split_tool_identity_is_buffered_until_complete() {
        let mut translator = StreamTranslator::new("resp_tool", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"arguments": "{\"a\":"}}]}}]}),
            &mut events,
        );
        assert!(events.is_empty());
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"name": "f", "arguments": "1}"}}]}}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        assert_eq!(values[0]["item"]["call_id"], "call_1");
        assert_eq!(values[0]["item"]["name"], "f");
        assert_eq!(values[1]["delta"], "{\"a\":1}");
    }

    #[test]
    fn conflicting_or_legacy_tool_calls_fail_instead_of_completing_empty() {
        let mut translator = StreamTranslator::new("resp_conflict", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_a", "function": {"name": "f", "arguments": ""}}]}}]}),
            &mut events,
        );
        let error = translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_b", "function": {"name": "f", "arguments": "{}"}}]}}]}),
            &mut events,
        );
        assert!(error.is_some());
        assert!(
            events_to_json(events)
                .iter()
                .any(|event| event["type"] == "response.failed")
        );

        let legacy = chat_to_responses_body(
            &json!({
                "choices": [{
                    "message": {"role": "assistant", "content": null, "function_call": {"name": "f", "arguments": "{}"}},
                    "finish_reason": "function_call"
                }]
            }),
            "resp_legacy",
            "model",
            1,
            &empty_reasoning_store(),
            false,
        );
        assert!(legacy.is_err());
    }

    #[test]
    fn buffered_sse_without_content_type_uses_the_normal_state_machine() {
        let events = translate_buffered_sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
            ResponseTranslationContext {
                response_id: "resp_buffered".into(),
                model: "model".into(),
                created_at: 1,
                store: Arc::new(empty_reasoning_store()),
                structured_output: false,
                structured_schema: None,
                metadata: ResponseMetadata::default(),
                capability_learning: None,
            },
        );
        let values = events_to_json(
            events
                .into_iter()
                .filter(|event| event.event_type != "done")
                .collect(),
        );
        assert_eq!(values.last().unwrap()["type"], "response.completed");
        assert_eq!(
            values.last().unwrap()["response"]["output"][0]["content"][0]["text"],
            "ok"
        );
    }

    #[test]
    fn buffered_sse_validates_schema_before_learning_prompt_capability() {
        let cache = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let events = translate_buffered_sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"ok\\\":\\\"wrong\\\"}\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
            ResponseTranslationContext {
                response_id: "resp_buffered_schema".into(),
                model: "model".into(),
                created_at: 1,
                store: Arc::new(empty_reasoning_store()),
                structured_output: true,
                structured_schema: Some(json!({
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}},
                    "required": ["ok"]
                })),
                metadata: ResponseMetadata::default(),
                capability_learning: Some(CapabilityLearning {
                    cache: cache.clone(),
                    key: "model\njson_schema".into(),
                }),
            },
        );
        let values = events_to_json(events);
        assert_eq!(values.last().unwrap()["type"], "response.failed");
        assert!(cache.lock().unwrap().is_empty());
    }

    #[test]
    fn sse_parser_joins_multi_line_data_and_dispatches_on_blank_line() {
        let mut parser = SseEventParser::default();
        assert!(
            parser
                .push_line("data: {\"id\": \"chatcmpl-1\",")
                .unwrap()
                .is_none()
        );
        assert!(
            parser
                .push_line("data: \"choices\": []}")
                .unwrap()
                .is_none()
        );
        let data = parser.push_line("").unwrap().unwrap();
        assert_eq!(data, "{\"id\": \"chatcmpl-1\",\n\"choices\": []}");
        // 注释与 event: 行被忽略。
        assert!(parser.push_line(": keep-alive").unwrap().is_none());
        assert!(parser.push_line("event: message").unwrap().is_none());
        assert!(parser.push_line("data: [DONE]").unwrap().is_none());
        assert_eq!(parser.push_line("").unwrap().unwrap(), "[DONE]");
        // EOF 时残留数据也会被分发。
        assert!(parser.push_line("data: {\"a\":1}").unwrap().is_none());
        assert_eq!(parser.finish().unwrap(), "{\"a\":1}");
        assert!(parser.finish().is_none());

        let mut parser = SseEventParser::default();
        assert!(
            parser
                .push_line("\u{feff}data: {\"ok\":true}")
                .unwrap()
                .is_none()
        );
        assert_eq!(parser.push_line("").unwrap().unwrap(), "{\"ok\":true}");
    }

    #[test]
    fn streaming_translator_emits_failed_event_on_error_chunk() {
        let mut translator = StreamTranslator::new("resp_z", "deepseek-chat", 1);
        let mut events = Vec::new();
        let error = translator
            .push_chunk(
                &json!({"error": {"message": "model overloaded", "type": "server_error"}}),
                &mut events,
            )
            .unwrap();
        assert_eq!(error, "model overloaded");
        let values = events_to_json(events);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["type"], "response.failed");
        assert_eq!(values[0]["response"]["status"], "failed");
        assert_eq!(
            values[0]["response"]["error"]["message"],
            "model overloaded"
        );
        assert_eq!(values[0]["response"]["error"]["code"], "server_error");
        assert!(values[0]["response"]["error"].get("type").is_none());
        for field in ["parallel_tool_calls", "tool_choice", "tools"] {
            assert!(values[0]["response"].get(field).is_some());
        }
    }

    #[test]
    fn streaming_usage_includes_all_required_details() {
        let mut translator = StreamTranslator::new("resp_usage", "model", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({
                "choices": [{"delta": {}, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "prompt_tokens_details": {"cached_tokens": 3, "cache_write_tokens": 2},
                    "completion_tokens_details": {"reasoning_tokens": 1}
                }
            }),
            &mut events,
        );
        assert!(translator.finish(&mut events, &empty_reasoning_store()));

        let values = events_to_json(events);
        let usage = &values.last().unwrap()["response"]["usage"];
        assert_eq!(usage["input_tokens_details"]["cached_tokens"], 3);
        assert_eq!(usage["input_tokens_details"]["cache_write_tokens"], 2);
        assert_eq!(usage["output_tokens_details"]["reasoning_tokens"], 1);
    }

    #[test]
    fn streaming_translator_keeps_reasoning_internal_without_phantom_item() {
        let store = empty_reasoning_store();
        let mut translator = StreamTranslator::new("resp_r", "deepseek-reasoner", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"reasoning_content": "先分析一下，"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"reasoning_content": "再给出答案。"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"content": "答案是 42"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "stop"}]}),
            &mut events,
        );
        translator.finish(&mut events, &store);

        let values = events_to_json(events);
        let types = values
            .iter()
            .map(|value| value["type"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        let message_added = values
            .iter()
            .find(|v| v["type"] == "response.output_item.added")
            .unwrap();
        assert_eq!(message_added["output_index"], 0);
        let completed = values.last().unwrap();
        assert_eq!(
            completed["response"]["output"][0]["content"][0]["text"],
            "答案是 42"
        );
        assert_eq!(
            store.lock().unwrap().get(&translator.msg_item_id),
            Some("先分析一下，再给出答案。")
        );
    }

    #[test]
    fn tool_call_without_arguments_is_still_emitted() {
        // 无参数工具调用：只有 id/name，没有 arguments 分片，也必须完整输出。
        let mut translator = StreamTranslator::new("resp_t", "deepseek-chat", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "get_time", "arguments": ""}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        let types = values
            .iter()
            .map(|value| value["type"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(types.contains(&"response.output_item.added"));
        assert!(types.contains(&"response.function_call_arguments.done"));
        assert!(types.contains(&"response.output_item.done"));
        let completed = values
            .iter()
            .find(|value| value["type"] == "response.completed")
            .unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        let call = output
            .iter()
            .find(|item| item["type"] == "function_call")
            .expect("无参数工具调用也必须出现在输出里");
        assert_eq!(call["name"], "get_time");
        assert_eq!(call["call_id"], "call_1");
    }

    #[test]
    fn stream_translator_persists_reasoning_for_its_output_items() {
        let store = empty_reasoning_store();
        let mut translator = StreamTranslator::new("resp_r", "deepseek-reasoner", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"reasoning_content": "第一步"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"content": "结果"}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "f", "arguments": "{}"}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            &mut events,
        );
        translator.finish(&mut events, &store);

        let guard = store.lock().unwrap();
        // 正文消息与工具调用条目都记住了同一份思考内容，供下一轮回传。
        assert_eq!(guard.get(&translator.msg_item_id), Some("第一步"));
        let tool_item_id = translator.tool_calls[&0].item_id.clone();
        assert_eq!(guard.get(&tool_item_id), Some("第一步"));
        drop(guard);
    }

    #[test]
    fn input_to_messages_attaches_remembered_reasoning_content() {
        let store = empty_reasoning_store();
        {
            let mut guard = store.lock().unwrap();
            guard.insert("msg_1", "第一步思考");
            guard.insert("fc_1", "工具调用前的思考");
        }
        let chat = responses_to_chat_body(
            &json!({
                "model": "deepseek-reasoner",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                    {"type": "message", "id": "msg_1", "role": "assistant", "content": [{"type": "output_text", "text": "好的"}]},
                    {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "f", "arguments": "{}"},
                    {"type": "function_call_output", "call_id": "call_1", "output": "done"}
                ]
            }),
            &store,
        );
        let messages = chat["messages"].as_array().unwrap();
        // Responses 的 assistant 文本与紧随其后的 function_call 属于同一轮，
        // 必须合并成一条 Chat assistant 消息并使用工具调用对应的 reasoning。
        let assistant_message = messages
            .iter()
            .find(|message| message.get("content").and_then(Value::as_str) == Some("好的"))
            .unwrap();
        assert!(assistant_message.get("tool_calls").is_some());
        assert_eq!(assistant_message["reasoning_content"], "工具调用前的思考");
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
                .count(),
            1
        );
    }

    #[test]
    fn reasoning_matches_by_call_id_when_item_id_is_rewritten() {
        // 条目 id 被 Codex 重写（重新生成）时，只要 call_id 一致，仍能回传思考内容。
        let store = empty_reasoning_store();
        {
            let mut guard = store.lock().unwrap();
            guard.insert("call_1", "工具调用前的思考");
        }
        let chat = responses_to_chat_body(
            &json!({
                "model": "deepseek-reasoner",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                    {"type": "function_call", "id": "regenerated_fc", "call_id": "call_1", "name": "f", "arguments": "{}"},
                    {"type": "function_call_output", "call_id": "call_1", "output": "done"}
                ]
            }),
            &store,
        );
        let messages = chat["messages"].as_array().unwrap();
        let tool_message = messages
            .iter()
            .find(|message| message.get("tool_calls").is_some())
            .unwrap();
        assert_eq!(tool_message["reasoning_content"], "工具调用前的思考");
    }

    #[test]
    fn ambiguous_call_ids_never_leak_reasoning_between_conversations() {
        let mut store = ReasoningStore::default();
        store.insert_call_alias("call_1", Arc::from("conversation A"));
        store.insert_call_alias("call_1", Arc::from("conversation B"));

        assert_eq!(store.get("call_1"), None);
    }

    #[test]
    fn non_streaming_response_persists_reasoning_content() {
        let store = empty_reasoning_store();
        let chat_response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "结果",
                    "reasoning_content": "思考过程",
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "f", "arguments": "{}"}}]
                },
                "finish_reason": "tool_calls"
            }]
        });
        let body = chat_to_responses_body(
            &chat_response,
            "resp_1",
            "deepseek-reasoner",
            0,
            &store,
            false,
        )
        .unwrap();
        let msg_id = body["output"][0]["id"].as_str().unwrap().to_owned();
        let guard = store.lock().unwrap();
        assert_eq!(guard.get(&msg_id), Some("思考过程"));
        // 工具调用条目同样记住（同一 suffix，按调用序号区分）。
        let fc_id = format!("fc_{}_0", msg_id.trim_start_matches("msg_"));
        assert_eq!(guard.get(&fc_id), Some("思考过程"));
    }

    #[test]
    fn sse_parser_accumulates_multiline_data_and_tolerates_cr() {
        let mut parser = SseEventParser::default();
        assert_eq!(parser.push_line("data: {\"type\":\"a\"").unwrap(), None);
        assert_eq!(parser.push_line("data: ,\"more\":1}").unwrap(), None);
        // event:/注释行忽略。
        assert_eq!(parser.push_line("event: x").unwrap(), None);
        assert_eq!(parser.push_line(": comment").unwrap(), None);
        assert_eq!(
            parser.push_line("").unwrap(),
            Some("{\"type\":\"a\"\n,\"more\":1}".into())
        );
        // 分发后缓冲区已取走，可复用。
        assert!(parser.data.is_empty());
        // 行尾 \r 容错。
        assert_eq!(parser.push_line("data: done\r").unwrap(), None);
        assert_eq!(parser.push_line("").unwrap(), Some("done".into()));
    }

    #[test]
    fn sse_parser_finish_dispatchs_remaining_data() {
        let mut parser = SseEventParser::default();
        assert_eq!(parser.push_line("data: tail").unwrap(), None);
        assert_eq!(parser.finish(), Some("tail".into()));
        assert_eq!(parser.finish(), None);
    }

    #[test]
    fn sse_parser_rejects_oversized_event_data() {
        let mut parser = SseEventParser::default();
        let line = format!("data: {}", "x".repeat(MAX_SSE_EVENT_DATA_BYTES + 1));
        assert!(parser.push_line(&line).is_err());
    }

    #[test]
    fn malformed_sse_json_emits_failed_not_completed() {
        let mut translator = StreamTranslator::new("resp_bad", "model", 1);
        let mut events = Vec::new();
        let mut failed = false;
        let mut done = false;
        assert!(dispatch_data(
            &mut translator,
            "{not-json}",
            &mut events,
            &mut failed,
            &mut done,
        ));
        let types = events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        assert!(types.contains(&"response.failed"));
        assert!(!types.contains(&"response.completed"));
    }

    #[test]
    fn streaming_translator_handles_tool_only_responses() {
        let mut translator = StreamTranslator::new("resp_y", "deepseek-chat", 1);
        let mut events = Vec::new();
        translator.push_chunk(
            &json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "f", "arguments": "{}"}}]}}]}),
            &mut events,
        );
        translator.push_chunk(
            &json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
            &mut events,
        );
        translator.finish(&mut events, &empty_reasoning_store());

        let values = events_to_json(events);
        let completed = values.last().unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["type"], "function_call");
        assert_eq!(output[0]["call_id"], "call_1");
    }

    #[tokio::test]
    async fn proxy_translates_non_streaming_requests_end_to_end() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            assert!(request.contains("chat/completions"));
            assert!(request.contains("Bearer secret"));
            assert!(request.contains("\"messages\""));
            let body = r#"{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"deepseek-chat","choices":[{"index":0,"message":{"role":"assistant","content":"你好"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "你好", "stream": false}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["object"], "response");
        assert_eq!(body["output"][0]["content"][0]["text"], "你好");
        assert_eq!(body["usage"]["input_tokens"], 3);
        upstream.join().unwrap();

        // 不同的服务复用同一个共享代理：监听端口固定，只切换上游配置。
        let registry = ChatProxyRegistry::default();
        let (port2, _) = registry.ensure(&provider).await.unwrap();
        let mut other = provider.clone();
        other.id = "p2".into();
        other.base_url = "http://127.0.0.1:2/v1".into();
        let (port3, _) = registry.ensure(&other).await.unwrap();
        assert_eq!(port2, port3);
        registry.stop_all().await;
    }

    #[tokio::test]
    async fn proxy_rejects_requests_without_credentials() {
        // 未携带本应用写入 Codex 的运行时凭证时直接 401，不转发上游。
        let provider = provider("p", "http://127.0.0.1:2/v1");
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .json(&json!({"model": "deepseek-chat", "input": "hi"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
        let response = client
            .get(format!("http://127.0.0.1:{port}/v1/models"))
            .bearer_auth("codex-tools")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
        // 携带错误凭证同样被拒绝。
        let response = client
            .get(format!("http://127.0.0.1:{port}/v1/models"))
            .bearer_auth("wrong-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
        let response = client
            .get(format!("http://127.0.0.1:{port}/v1/models"))
            .bearer_auth(&proxy_api_key)
            .send()
            .await
            .unwrap();
        assert_ne!(response.status().as_u16(), 401);
    }

    #[tokio::test]
    async fn proxy_forwards_upstream_errors() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let body = r#"{"error":{"message":"invalid api key","type":"authentication_error","code":"invalid_api_key"}}"#;
            write!(
                stream,
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "hi"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"]["message"], "invalid api key");
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_learns_prompt_structured_output_after_validation_rejection() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            // 第一次请求：带 response_format，上游拒绝结构化输出。
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            assert!(request.contains("response_format"));
            let error_body = r#"{"error":{"code":400,"message":"invalid request","type":"invalid_request_error"}}"#;
            write!(
                stream,
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                error_body.len(),
                error_body
            )
            .unwrap();
            // 第二次请求：降级后不带 response_format，schema 写进 system 提示词。
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            assert!(!request.contains("response_format"));
            assert!(request.contains("JSON Schema"));
            assert!(!request.contains("top-level fields: ok"));
            let body = r#"{"id":"chatcmpl-2","object":"chat.completion","created":1700000000,"model":"deepseek-chat","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ok\": true}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            // The learned capability applies to later requests for the same
            // upstream/model, so they skip the known-invalid native attempt.
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            assert!(!request.contains("response_format"));
            assert!(request.contains("JSON Schema"));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({
                "model": "deepseek-chat",
                "input": "hi",
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "review",
                        "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
                    }
                }
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["output"][0]["content"][0]["text"], "{\"ok\":true}");
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({
                "model": "deepseek-chat",
                "input": "hi again",
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "review",
                        "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
                    }
                }
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_translates_streaming_requests_end_to_end() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let chunks = [
                r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"role":"assistant","content":"你"}}]}"#,
                r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"content":"好"}}]}"#,
                r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
                r#"data: {"id":"chatcmpl-1","usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#,
                "data: [DONE]",
            ]
            .join("\n\n");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                chunks.len(),
                chunks
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "你好", "stream": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
            "text/event-stream"
        );
        let text = response.text().await.unwrap();
        let types = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .filter_map(|value| value["type"].as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                "response.created",
                "response.in_progress",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        assert!(!text.contains("data: [DONE]"));
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_streams_many_tokens_without_loss_or_reordering() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let total = 2000usize;
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let header =
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
            stream.write_all(header.as_bytes()).unwrap();
            // 大量 token 分小块写入，验证逐块解析不丢 token、顺序正确。
            for i in 0..total {
                let data = format!(
                    "data: {{\"id\":\"c\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{i}\"}}}}]}}\n\n"
                );
                stream.write_all(data.as_bytes()).unwrap();
                if i % 50 == 0 {
                    stream.flush().unwrap();
                }
            }
            stream.write_all(b"data: [DONE]\n\n").unwrap();
            stream.flush().unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        let deltas = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|data| *data != "[DONE]")
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .filter_map(|value| {
                value
                    .get("delta")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        // 每个 content delta 都收到且顺序正确（无丢失、无乱序）。
        assert_eq!(deltas.len(), total);
        for (index, delta) in deltas.iter().enumerate() {
            assert_eq!(delta, &index.to_string());
        }
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn reasoning_content_is_passed_back_on_next_turn_end_to_end() {
        // 模拟 DeepSeek 推理模型的两轮工具调用：第一轮流式返回 reasoning + 工具调用，
        // 第二轮必须把 reasoning_content 回传给上游（DeepSeek 的硬性要求）。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            // 第一轮：接收请求，返回流式 reasoning + 工具调用。
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let sse = [
                r#"data: {"id":"c","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"第一步思考"}}]}"#,
                r#"data: {"id":"c","choices":[{"index":0,"delta":{"content":"好的"}}]}"#,
                r#"data: {"id":"c","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"run_shell","arguments":"{\"cmd\":\"ls\"}"}}]}}]}"#,
                r#"data: {"id":"c","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
                r#"data: {"id":"c","usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}"#,
                "data: [DONE]",
            ]
            .join("\n\n");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{}",
                sse
            )
            .unwrap();
            drop(stream);
            // 第二轮：必须收到带 reasoning_content 的 assistant 工具调用消息。
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            let body: Value =
                serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap_or("{}")).unwrap();
            let messages = body["messages"].as_array().unwrap();
            let tool_message = messages
                .iter()
                .find(|message| message.get("tool_calls").is_some())
                .expect("第二轮应包含工具调用 assistant 消息");
            assert_eq!(
                tool_message["reasoning_content"], "第一步思考",
                "工具调用消息必须回传 reasoning_content"
            );
            let reply = r#"{"id":"chatcmpl-2","object":"chat.completion","created":1700000000,"model":"deepseek-reasoner","choices":[{"index":0,"message":{"role":"assistant","content":"完成"},"finish_reason":"stop"}],"usage":{"prompt_tokens":6,"completion_tokens":2,"total_tokens":8}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                reply.len(),
                reply
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let base = format!("http://127.0.0.1:{port}/v1/responses");
        // 第一轮：流式请求，拿到输出条目（含代理生成的 id）。
        let response = client
            .post(&base)
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-reasoner", "input": "运行命令", "stream": true}))
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        let completed = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .find(|value| value["type"] == "response.completed")
            .expect("第一轮应收到 response.completed");
        let output = completed["response"]["output"].as_array().unwrap();
        let message_item = output
            .iter()
            .find(|item| item["type"] == "message")
            .unwrap();
        let function_item = output
            .iter()
            .find(|item| item["type"] == "function_call")
            .unwrap();
        let msg_id = message_item["id"].as_str().unwrap();
        let fc_id = function_item["id"].as_str().unwrap();
        let call_id = function_item["call_id"].as_str().unwrap();
        // 第二轮：带历史继续请求（id 与第一轮响应一致）。
        let response = client
            .post(&base)
            .bearer_auth(&proxy_api_key)
            .json(&json!({
                "model": "deepseek-reasoner",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "运行命令"}]},
                    {"type": "message", "id": msg_id, "role": "assistant", "content": [{"type": "output_text", "text": "好的"}]},
                    {"type": "function_call", "id": fc_id, "call_id": call_id, "name": "run_shell", "arguments": "{\"cmd\":\"ls\"}"},
                    {"type": "function_call_output", "call_id": call_id, "output": "ok"}
                ]
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_fails_when_upstream_closes_without_a_terminal_choice() {
        // Transport EOF without a terminal finish_reason is a truncated model response.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let chunks = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"世界\"}}]}";
            write!(stream, "{}", chunks).unwrap();
            drop(stream);
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        let values = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .collect::<Vec<_>>();
        let types = values
            .iter()
            .map(|value| value["type"].as_str().unwrap_or("").to_owned())
            .collect::<Vec<_>>();
        assert!(types.contains(&"response.failed".to_owned()));
        assert!(!types.contains(&"response.completed".to_owned()));
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_fails_stream_with_oversized_unterminated_line() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
                .unwrap();
            stream
                .write_all(&vec![b'x'; MAX_SSE_LINE_BUFFER_BYTES + 1])
                .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let text = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "m", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(text.contains("response.failed"));
        assert!(!text.contains("response.completed"));
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_fails_when_eof_line_pushes_event_data_over_limit() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
                .unwrap();
            write!(
                stream,
                "data: {}\ndata: tail",
                "x".repeat(MAX_SSE_EVENT_DATA_BYTES - 4)
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let text = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "m", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(text.contains("response.failed"));
        assert!(!text.contains("response.completed"));
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_rejects_oversized_non_streaming_response() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                crate::provider_http::MAX_UPSTREAM_BODY_BYTES + 1
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "m", "input": "hi", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn registry_keeps_port_and_swaps_config_when_parameters_change() {
        let registry = ChatProxyRegistry::default();
        let provider = provider("p", "http://127.0.0.1:1/v1");
        let first = registry.ensure(&provider).await.unwrap();
        assert_eq!(registry.ensure(&provider).await.unwrap(), first);
        // 修改上游地址/Key 后端口必须保持不变，否则 Codex 缓存的地址会失效。
        let mut changed = provider.clone();
        changed.base_url = "http://127.0.0.1:2/v1".into();
        changed.api_key = Some("new-secret".into());
        let second = registry.ensure(&changed).await.unwrap();
        assert_eq!(first, second);
        // 配置确实已更新：代理会带着新 Key 请求新的上游地址。
        let slot = registry
            .single
            .lock()
            .await
            .as_ref()
            .unwrap()
            .config
            .clone();
        let config = slot.current.read().await.config.clone();
        assert_eq!(config.upstream_base, "http://127.0.0.1:2/v1");
        assert_eq!(config.api_key, "new-secret");
        assert_eq!(PROXY_PORT, 27777);
        registry.stop_all().await;
    }

    #[tokio::test]
    async fn equivalent_provider_switch_updates_owner_and_clears_reasoning() {
        let registry = ChatProxyRegistry::default();
        let first = provider("first", "http://127.0.0.1:1/v1");
        registry.ensure(&first).await.unwrap();
        let slot = registry
            .single
            .lock()
            .await
            .as_ref()
            .unwrap()
            .config
            .clone();
        let old_store = slot.current.read().await.reasoning_store.clone();
        old_store
            .lock()
            .unwrap()
            .insert("call_1", "private reasoning");

        let mut second = first.clone();
        second.id = "second".into();
        registry.ensure(&second).await.unwrap();
        // Equivalent connection details still transfer ownership.
        registry.stop("first").await;
        assert!(!slot.current.read().await.config.is_disabled());
        let current_store = slot.current.read().await.reasoning_store.clone();
        assert!(!Arc::ptr_eq(&old_store, &current_store));
        assert!(current_store.lock().unwrap().get("call_1").is_none());
        // An old in-flight response may still finish, but it only writes its detached store.
        old_store
            .lock()
            .unwrap()
            .insert("late_call", "old provider reasoning");
        assert!(current_store.lock().unwrap().get("late_call").is_none());

        let mut changed = second.clone();
        changed.base_url = "http://127.0.0.1:2/v1".into();
        registry.ensure(&changed).await.unwrap();
        assert!(
            slot.current
                .read()
                .await
                .reasoning_store
                .lock()
                .unwrap()
                .get("call_1")
                .is_none()
        );
        registry.stop("second").await;
        assert!(slot.current.read().await.config.is_disabled());
        registry.stop_all().await;
    }

    #[tokio::test]
    async fn deleting_config_owner_disables_upstream_until_next_switch() {
        let registry = ChatProxyRegistry::default();
        let first = provider("p", "http://127.0.0.1:1/v1");
        let (port, token) = registry.ensure(&first).await.unwrap();
        // 删除当前配置所属的服务后，配置被清空但监听仍在（端口不变）。
        registry.stop("p").await;
        let slot = registry
            .single
            .lock()
            .await
            .as_ref()
            .unwrap()
            .config
            .clone();
        assert!(slot.current.read().await.config.is_disabled());
        // 切换到其他服务后配置恢复。
        let other = provider("p2", "http://127.0.0.1:2/v1");
        assert_eq!(registry.ensure(&other).await.unwrap(), (port, token));
        let slot = registry
            .single
            .lock()
            .await
            .as_ref()
            .unwrap()
            .config
            .clone();
        assert_eq!(
            slot.current.read().await.config.upstream_base,
            "http://127.0.0.1:2/v1"
        );
        registry.stop_all().await;
    }

    #[tokio::test]
    async fn effective_url_uses_proxy_for_chat_and_direct_for_responses() {
        let registry = ChatProxyRegistry::default();
        let chat = provider("chat", "https://api.deepseek.com/v1");
        let target = effective_base_url(&chat, &registry).await.unwrap();
        assert!(target.base_url.starts_with("http://127.0.0.1:"));
        assert!(target.base_url.ends_with("/v1"));
        assert!(target.proxy_api_key.is_some());

        let mut responses = chat.clone();
        responses.api_type = ProviderApiType::Responses;
        let target = effective_base_url(&responses, &registry).await.unwrap();
        assert_eq!(target.base_url, "https://api.deepseek.com/v1");
        assert!(target.proxy_api_key.is_none());
        registry.stop_all().await;
    }

    #[tokio::test]
    async fn registries_generate_distinct_proxy_tokens() {
        let provider = provider("p", "http://127.0.0.1:1/v1");
        let first = ChatProxyRegistry::default();
        let second = ChatProxyRegistry::default();
        let (_, first_token) = first.ensure(&provider).await.unwrap();
        let (_, second_token) = second.ensure(&provider).await.unwrap();

        assert_ne!(first_token, second_token);
        assert!(first_token.len() >= 108);
        first.stop_all().await;
        second.stop_all().await;
    }

    #[test]
    fn upstream_headers_skip_invalid_entries() {
        // 异常请求头只跳过错误项，不影响其余有效头（保存时已校验，这里是兜底）。
        let headers = build_upstream_headers(vec![
            ("X-Custom".into(), "ok".into()),
            ("User-Agent".into(), "custom-agent".into()),
            ("invalid header".into(), "x".into()),
            ("X-Other".into(), "bad\nvalue".into()),
        ]);
        assert_eq!(
            headers
                .get("x-custom")
                .and_then(|value| value.to_str().ok()),
            Some("ok")
        );
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn completion_to_chunk_indexes_multiple_tool_calls() {
        // 非流式补全的 tool_calls 不带 index，必须按位置补上，
        // 否则多个工具调用会被折叠成一个。
        let chunk = completion_to_chunk(&json!({
            "choices": [{"message": {"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "a", "arguments": "{}"}},
                {"id": "call_2", "type": "function", "function": {"name": "b", "arguments": "{}"}}
            ]}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        }));
        let delta = &chunk["choices"][0]["delta"];
        let calls = delta["tool_calls"].as_array().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["index"], 0);
        assert_eq!(calls[1]["index"], 1);
        // content 为 null（纯工具调用）时不写入 delta，避免产生空文本事件。
        assert!(delta.get("content").is_none());
        assert_eq!(chunk["usage"]["prompt_tokens"], 1);
    }

    #[tokio::test]
    async fn proxy_synthesizes_sse_when_upstream_ignores_stream_request() {
        // 上游忽略 stream=true，直接返回完整 JSON 补全：
        // 代理必须合成等价的 SSE 事件流，保证 Codex 侧协议一致。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let body = r#"{"id":"chatcmpl-9","object":"chat.completion","created":1700000000,"model":"deepseek-chat","choices":[{"index":0,"message":{"role":"assistant","content":"一次性结果","tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":6,"total_tokens":10}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "deepseek-chat", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
            "text/event-stream"
        );
        let text = response.text().await.unwrap();
        let values = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .collect::<Vec<_>>();
        let completed = values
            .iter()
            .find(|value| value["type"] == "response.completed")
            .expect("兜底流必须以 response.completed 收尾");
        let output = completed["response"]["output"].as_array().unwrap();
        assert_eq!(output[0]["content"][0]["text"], "一次性结果");
        let call = output
            .iter()
            .find(|item| item["type"] == "function_call")
            .expect("工具调用必须出现在合成流的输出里");
        assert_eq!(call["call_id"], "call_1");
        assert_eq!(call["arguments"], "{\"a\":1}");
        assert_eq!(completed["response"]["usage"]["input_tokens"], 4);
        assert_eq!(completed["response"]["usage"]["output_tokens"], 6);
        assert!(!text.contains("data: [DONE]"));
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_surfaces_json_error_when_stream_request_gets_non_sse_error() {
        // 部分网关流式请求失败时返回 200 + JSON 错误，必须转成明确的错误响应。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            let body = r#"{"error":{"message":"bad model","type":"invalid_request_error"}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let provider = provider("p", &format!("http://{address}"));
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "bad", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 502);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"]["message"], "bad model");
        upstream.join().unwrap();
    }

    #[tokio::test]
    async fn proxy_times_out_when_upstream_stalls_before_responding() {
        // 上游接受连接但永远不返回任何数据：非流式按整体超时、
        // 流式按首字节超时，都必须以 504 收尾而不是无限悬挂。
        for stream in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let upstream = std::thread::spawn(move || {
                use std::io::Read;
                let (mut stream, _) = listener.accept().unwrap();
                // 保持连接但不返回任何数据：持续读到 EOF（代理超时断开）为止。
                let mut buf = [0_u8; 1024];
                while stream.read(&mut buf).map(|read| read > 0).unwrap_or(false) {}
            });
            let mut provider = provider("p", &format!("http://{address}"));
            provider.timeout_secs = 1;
            let (port, _shutdown, _slot, proxy_api_key) =
                start_proxy(ProxyConfig::from_provider(&provider))
                    .await
                    .unwrap();
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let response = client
                .post(format!("http://127.0.0.1:{port}/v1/responses"))
                .bearer_auth(&proxy_api_key)
                .json(&json!({"model": "m", "input": "hi", "stream": stream}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 504, "stream={stream}");
            let body: Value = response.json().await.unwrap();
            assert!(
                body["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("超时"),
                "stream={stream} 应返回明确的超时提示"
            );
            upstream.join().unwrap();
        }
    }

    #[tokio::test]
    async fn proxy_returns_gateway_timeout_when_non_streaming_body_stalls() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let _request = read_http_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{")
                .unwrap();
            let mut response = [0_u8; 1024];
            while stream
                .read(&mut response)
                .map(|read| read > 0)
                .unwrap_or(false)
            {}
        });
        let mut provider = provider("p", &format!("http://{address}"));
        provider.timeout_secs = 1;
        let (port, _shutdown, _slot, proxy_api_key) =
            start_proxy(ProxyConfig::from_provider(&provider))
                .await
                .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .post(format!("http://127.0.0.1:{port}/v1/responses"))
            .bearer_auth(&proxy_api_key)
            .json(&json!({"model": "m", "input": "hi", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        upstream.join().unwrap();
    }
}
