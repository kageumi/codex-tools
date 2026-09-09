//! 通过 Chrome DevTools Protocol (CDP) 解锁 Codex 桌面应用的模型列表。
//!
//! 原理参考 codex++：Codex 桌面应用基于 Electron，用
//! `--remote-debugging-port` 启动后会在本机 `127.0.0.1` 暴露 CDP 端点。
//! 本模块连接该端点，在渲染进程注入脚本，把模型选择器的白名单补齐为
//! **当前激活服务商**的模型列表。
//!
//! 解锁目录不再内置任何模型：模型数据来自当前服务商 `/models` 接口返回的
//! 可用模型（保存服务时静默获取），只包含当前服务商实际存在的模型，
//! 不含内置 GPT 模型。
//!
//! 桌面应用的模型选择器有两个数据源：
//! 1. `config.toml` 的 `model_catalog_json` —— 让内嵌 CLI 的
//!    `list-models-for-host` 返回自定义模型（见 [`write_model_catalog`]）；
//! 2. Statsig 动态配置 `107580212` 的 `available_models` —— 决定选择器显示
//!    哪些模型。注入脚本会在 Statsig SDK 初始化前挂上 setter 钩子，保证
//!    补丁先于应用的配置读取生效。
//!
//! 脚本只在 Codex 渲染进程的内存中生效，不修改 Codex 安装文件；重启后
//! 注入丢失（此时选择器仍可通过 `model_catalog_json` 列出模型，但会受
//! 订阅白名单限制），需要重新注入。

use crate::{
    models::{
        ActiveKind, AppError, CodexModelInfo, ModelUnlockResult, ModelUnlockStatus,
        ProviderProfile, ReasoningLevelInfo,
    },
    platform,
    state::ActivationLock,
    storage::Store,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::MutexGuard,
};
use tokio_tungstenite::tungstenite::Message;

/// 写入 Codex 配置目录的模型目录文件名（相对 `config.toml` 所在目录）。
pub(crate) const MODEL_CATALOG_DIR: &str = "model-catalogs";
pub(crate) const MODEL_CATALOG_FILE: &str = "codex-tools.json";
/// 探测已有 CDP 端口的候选列表：上次随机端口（运行时拼入）以及
/// 常见调试端口（codex++ 9229、常规 DevTools 端口等），便于复用用户
/// 已开启的调试实例。不再包含固定端口：本应用每次都以随机端口启动，
/// 避免端口可被本机其他进程预测。
const PROBE_PORTS: &[u16] = &[9229, 9222, 9334, 9335];
const PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const MAX_CDP_PROBE_BYTES: usize = 256 * 1024;
const EVALUATE_TIMEOUT: Duration = Duration::from_secs(5);
const WAIT_LAUNCH_TIMEOUT: Duration = Duration::from_secs(30);

/// 构造调试端口候选列表：上次启动用的随机端口优先，再补上常见调试端口。
/// 端口不再固定，本机其他进程无法预测本应用的调试端口。
fn debug_probe_ports(store: &Store) -> Vec<u16> {
    let mut ports = Vec::with_capacity(PROBE_PORTS.len() + 1);
    if let Ok(Some(last)) = store.last_debug_port() {
        ports.push(last);
    }
    ports.extend_from_slice(PROBE_PORTS);
    ports
}

/// 分配一个随机的本机端口：让系统从临时端口范围挑选（借用后立即释放），
/// 避免固定端口被本机其他进程探测/复用。
fn pick_debug_port() -> u16 {
    std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|address| address.port())
        .unwrap_or(PROBE_PORTS[0])
}
/// 未匹配到任何元数据时使用的上下文窗口：取最保守、最兼容的 128K，
/// 避免高估模型上下文导致溢出。
const DEFAULT_CONTEXT_WINDOW: u64 = 128_000;
const DEFAULT_REASONING_LEVELS: &[&str] = &["low", "medium", "high", "xhigh"];
/// Codex CLI 的 `model_catalog_json` 要求 `base_instructions` 为字符串。
/// 保持通用、不绑定具体模型（不写“based on GPT-5”），以适配软件更新。
const CODEX_BASE_INSTRUCTIONS: &str =
    "You are Codex, a coding agent. You and the user share one workspace.";

/// 构建注入到 Codex 的模型目录：**只包含当前激活服务商**实际存在的模型。
///
/// 数据来源（均来自该服务商本身，不内置任何模型）：
/// 1. 服务 `/models` 接口返回的可用模型（保存服务时静默获取，`available_models`）；
/// 2. 服务 `/models` 接口返回的模型上下文窗口（`model_context_windows`）；
///
/// 未启用第三方服务（官方 OpenAI 或无激活服务）时返回空目录：官方账号的
/// 模型选择器由订阅等级控制，不注入默认 GPT 模型。
pub(crate) fn model_catalog(store: &Store) -> Result<Vec<CodexModelInfo>, AppError> {
    let active = store.read(|state| state.active.clone())?;
    let Some(provider_id) = active
        .provider_id
        .filter(|_| matches!(active.kind, ActiveKind::Provider))
    else {
        return Ok(Vec::new());
    };
    let provider = store.provider(&provider_id)?;
    Ok(build_provider_catalog(&provider))
}

/// 所有会写模型目录或注入页面的入口都通过这个上下文统一采用
/// model transaction → activation 的锁序，并在拿到两把锁后重新读取 active。
struct ModelCatalogWriteContext<'a> {
    _model_transaction: MutexGuard<'a, ()>,
    _activation: MutexGuard<'a, ()>,
    active_kind: ActiveKind,
    catalog: Vec<CodexModelInfo>,
}

async fn model_catalog_write_context<'a>(
    store: &Store,
    activation: &'a ActivationLock,
) -> Result<ModelCatalogWriteContext<'a>, AppError> {
    let model_transaction = activation.2.lock().await;
    let activation_guard = activation.0.lock().await;
    let active_kind = store.read(|state| state.active.kind)?;
    let catalog = model_catalog(store)?;
    Ok(ModelCatalogWriteContext {
        _model_transaction: model_transaction,
        _activation: activation_guard,
        active_kind,
        catalog,
    })
}

/// 用单个服务的模型记录构建目录（只含该服务实际存在的模型）。
pub(crate) fn build_model_catalog_with_windows_for_provider(
    provider: &ProviderProfile,
) -> Vec<CodexModelInfo> {
    build_provider_catalog(provider)
}

fn build_provider_catalog(provider: &ProviderProfile) -> Vec<CodexModelInfo> {
    let mut by_slug = BTreeMap::new();
    // 有效模型 = /models 同步的 available_models ∪ 用户手动添加的 custom_models。
    // 展示名/上下文窗口/简介用 models.dev(catalog.json) 精确匹配补充；自定义模型
    // 无元数据时回退默认窗口并以 slug 作为展示名。
    let available: std::collections::HashSet<&String> = provider.available_models.iter().collect();
    let custom: std::collections::HashSet<&String> = provider.custom_models.iter().collect();
    let models: Vec<&String> = match provider.selected_models.as_deref() {
        Some([]) => Vec::new(),
        Some(selected) => selected
            .iter()
            .filter(|model| available.contains(model) || custom.contains(model))
            .collect(),
        None => provider
            .available_models
            .iter()
            .chain(provider.custom_models.iter())
            .collect(),
    };
    for slug in models {
        let slug = slug.trim();
        if slug.is_empty() {
            continue;
        }
        let meta = provider.models_dev_meta.get(slug);
        let context_window = provider.context_window_override.unwrap_or_else(|| {
            provider
                .model_context_windows
                .get(slug)
                .copied()
                .or_else(|| meta.and_then(|meta| meta.context_window))
                .unwrap_or(DEFAULT_CONTEXT_WINDOW)
        });
        let display_name = meta
            .and_then(|meta| meta.name.clone())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| slug.to_string());
        by_slug.insert(
            slug.to_string(),
            catalog_entry(
                slug,
                &display_name,
                context_window,
                meta.and_then(|meta| meta.description.clone()),
                false,
            ),
        );
    }
    by_slug.into_values().collect()
}

fn catalog_entry(
    slug: &str,
    display_name: &str,
    context_window: u64,
    description: Option<String>,
    reasoning: bool,
) -> CodexModelInfo {
    CodexModelInfo {
        slug: slug.to_string(),
        display_name: display_name.to_string(),
        description: description.filter(|value| !value.trim().is_empty()),
        context_window: Some(context_window),
        max_context_window: Some(context_window),
        default_reasoning_level: Some((if reasoning { "medium" } else { "low" }).to_string()),
        supported_reasoning_levels: Some(default_reasoning_levels()),
        base_instructions: Some(CODEX_BASE_INSTRUCTIONS.into()),
        visibility: Some("list".into()),
        supported_in_api: Some(true),
        priority: Some(1),
        shell_type: Some("shell_command".into()),
        ..CodexModelInfo::default()
    }
}

/// 把目录写入 `<codex_home>/model-catalogs/codex-tools.json`，供
/// `config.toml` 的 `model_catalog_json` 指向；返回相对 config.toml 的路径。
/// 原子写入：先写临时文件再替换，避免中途崩溃留下半截 JSON。
pub(crate) fn write_model_catalog(
    codex_home: &Path,
    catalog: &[CodexModelInfo],
) -> Result<String, AppError> {
    let dir = codex_home.join(MODEL_CATALOG_DIR);
    fs::create_dir_all(&dir)?;
    let path = dir.join(MODEL_CATALOG_FILE);
    let mut json = serde_json::to_vec_pretty(&json!({ "models": catalog }))
        .map_err(|error| AppError::Internal(error.to_string()))?;
    json.push(b'\n');
    crate::storage::atomic_write(&path, &json).map_err(AppError::from)?;
    Ok(format!("{MODEL_CATALOG_DIR}/{MODEL_CATALOG_FILE}"))
}

fn default_reasoning_levels() -> Vec<ReasoningLevelInfo> {
    DEFAULT_REASONING_LEVELS
        .iter()
        .map(|effort| ReasoningLevelInfo {
            effort: (*effort).to_string(),
            description: Some(format!("{effort} effort")),
        })
        .collect()
}

/// 查询解锁状态：Codex 是否安装/运行、是否有可注入的调试端口、
/// 解锁脚本是否已生效，以及当前目录包含哪些模型。
pub(crate) async fn status(store: &Store) -> Result<ModelUnlockStatus, AppError> {
    let active_kind = store.read(|state| state.active.kind)?;
    let models = model_catalog(store)?;
    let debug_port = find_codex_debug_port_on(&debug_probe_ports(store)).await;
    let injected = match debug_port {
        Some(port) => match codex_page_ws_url(port).await {
            Some(ws_url) => evaluate(&ws_url, "window.__CODEX_TOOLS_MODEL_UNLOCKED__ === true")
                .await
                .map(|value| value.as_bool().unwrap_or(false))
                .unwrap_or(false),
            None => false,
        },
        None => false,
    };
    let configured = store.codex_app_setting()?;
    let app_found = platform::codex_app_found(configured.as_deref());
    let app_running = platform::codex_app_running(configured.as_deref());
    let catalog_warning = match active_kind {
        ActiveKind::None => {
            Some("尚未启用任何 API 服务，请先在“账号与服务”添加并启用一个服务。".into())
        }
        ActiveKind::Official => {
            Some("当前使用 OpenAI 官方账号，模型选择器由订阅等级控制，无需解锁。".into())
        }
        ActiveKind::Provider if models.is_empty() => {
            Some("当前服务商没有可用模型，请编辑服务并保存（保存时自动获取服务端模型）。".into())
        }
        ActiveKind::Provider => None,
    };
    let warning = if !app_found {
        Some("未找到 Codex 桌面应用，请在下方“Codex 应用”中手动选择，或安装 Codex 桌面版。".into())
    } else if catalog_warning.is_some() {
        catalog_warning
    } else if app_running && debug_port.is_none() {
        Some(
            "Codex 正在运行，但没有开启调试端口。为避免影响现有会话，请手动退出 Codex 后，再从概览页点击“打开 Codex（自动解锁）”。"
                .into(),
        )
    } else if debug_port.is_none() {
        Some("Codex 未运行。从概览页点击“打开 Codex（自动解锁）”即可启动并解锁模型列表。".into())
    } else {
        None
    };
    Ok(ModelUnlockStatus {
        app_found,
        app_running,
        debug_port,
        injected,
        model_count: models.len(),
        models: models.into_iter().map(|model| model.slug).collect(),
        warning,
    })
}

/// 向已开启调试端口的 Codex 实例注入模型目录与解锁脚本。
pub(crate) async fn unlock(
    store: &Store,
    activation: &ActivationLock,
) -> Result<ModelUnlockResult, AppError> {
    unlock_on(store, activation, &debug_probe_ports(store)).await
}

async fn unlock_on(
    store: &Store,
    activation: &ActivationLock,
    ports: &[u16],
) -> Result<ModelUnlockResult, AppError> {
    let context = model_catalog_write_context(store, activation).await?;
    let port = find_codex_debug_port_on(ports).await.ok_or_else(|| {
        AppError::InvalidConfig(
            "未找到带调试端口的 Codex 实例，请先用“以调试模式启动 Codex 并解锁”打开。".into(),
        )
    })?;
    let active_kind = context.active_kind;
    let catalog = context.catalog;
    if catalog.is_empty() {
        return Ok(ModelUnlockResult {
            port,
            injected: false,
            model_count: 0,
            message: match active_kind {
                ActiveKind::Official => {
                    "当前使用 OpenAI 官方账号，模型选择器由订阅等级控制，无需解锁。".into()
                }
                ActiveKind::None => "尚未启用任何 API 服务，添加并启用服务后再解锁。".into(),
                ActiveKind::Provider => {
                    "当前服务商没有可用模型，请编辑服务并保存（保存时自动获取服务端模型）。".into()
                }
            },
        });
    }
    // 刷新 model_catalog_json 指向的目录文件，保证已运行实例也能读到自定义模型。
    let home = crate::codex::home(&store.codex_home_setting()?);
    write_model_catalog(&home, &catalog)?;
    inject(port, &catalog).await
}

/// 启动 Codex 桌面应用（调试模式）并默认解锁模型；
/// 已在调试端口运行的实例直接重新注入；
/// 已运行但没有调试端口的实例不会被关闭或重启，而是提示用户手动退出；
/// 官方账号/无激活服务时只启动 Codex、不注入（由订阅等级或手动配置决定）。
/// 启动前确保 `model_catalog_json` 指向的目录文件已写入（让 CLI 的
/// `list-models-for-host` 返回自定义模型），并尽快注入（让 Statsig 补丁先于
/// 应用读取白名单配置）。
pub(crate) async fn launch_with_debug(
    store: &Store,
    activation: &ActivationLock,
) -> Result<ModelUnlockResult, AppError> {
    let context = model_catalog_write_context(store, activation).await?;
    let active_kind = context.active_kind;
    ensure_active_official_account_usable(store, active_kind)?;
    let catalog = context.catalog;
    let configured = store.codex_app_setting()?;
    let empty_message = |active_kind| match active_kind {
        ActiveKind::Official => {
            "Codex 已启动。官方账号的模型选择器由订阅等级控制，无需解锁。".to_string()
        }
        ActiveKind::None => {
            "Codex 已启动。尚未启用任何 API 服务，添加并启用服务后再解锁。".to_string()
        }
        ActiveKind::Provider => {
            "Codex 已启动。当前服务商没有可用模型，请编辑服务并保存后重试。".to_string()
        }
    };
    // 已有调试端口的运行实例：直接重新注入，不触碰运行中的进程。
    if let Some(port) = find_codex_debug_port_on(&debug_probe_ports(store)).await {
        let home = crate::codex::home(&store.codex_home_setting()?);
        write_model_catalog(&home, &catalog)?;
        if catalog.is_empty() {
            return Ok(ModelUnlockResult {
                port,
                injected: false,
                model_count: 0,
                message: empty_message(active_kind),
            });
        }
        return inject(port, &catalog).await;
    }
    // 单实例桌面应用通常会忽略第二次启动传入的调试参数；不关闭或重启
    // 用户现有的实例，直接要求用户手动退出后再启动。
    if platform::codex_app_running(configured.as_deref()) {
        return Err(AppError::InvalidConfig(
            "Codex 已在运行但没有开启调试端口。请先完全退出 Codex（任务管理器中结束进程），再从概览页点击「打开 Codex（自动解锁）」。".into(),
        ));
    }
    let home = crate::codex::home(&store.codex_home_setting()?);
    write_model_catalog(&home, &catalog)?;
    // 每次启动使用随机端口并持久化，避免固定端口可被本机其他进程预测；
    // 探测时校验目标必须是 Codex 页面（app://-），不会误连其他调试端点。
    let debug_port = pick_debug_port();
    store.save_last_debug_port(debug_port)?;
    platform::dashboard_launch_app_with_debug(debug_port, configured.as_deref())
        .map_err(|error| AppError::Internal(format!("无法以调试模式启动 Codex：{error}")))?;
    let deadline = Instant::now() + WAIT_LAUNCH_TIMEOUT;
    let port = loop {
        if let Some(port) = find_codex_debug_port_on(&debug_probe_ports(store)).await {
            break port;
        }
        if Instant::now() >= deadline {
            let hint = if platform::codex_app_running(configured.as_deref()) {
                "Codex 似乎已启动但未开启调试端口。请确认 Codex 是通过本工具启动的（而非手动双击），或手动退出后重试。"
            } else {
                "Codex 未能正常启动。请检查 Codex 路径是否正确（设置页 → Codex 应用），或手动启动 Codex 后再试。"
            };
            return Err(AppError::InvalidConfig(format!(
                "等待 Codex 调试端口超时（30 秒）。{hint}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    if catalog.is_empty() {
        return Ok(ModelUnlockResult {
            port,
            injected: false,
            model_count: 0,
            message: empty_message(active_kind),
        });
    }
    inject(port, &catalog).await
}

fn ensure_active_official_account_usable(
    store: &Store,
    active_kind: ActiveKind,
) -> Result<(), AppError> {
    if !matches!(active_kind, ActiveKind::Official) {
        return Ok(());
    }
    let account_id = store
        .read(|state| state.active.account_id.clone())?
        .ok_or_else(|| {
            AppError::InvalidConfig("当前 OpenAI 登录信息不完整，请重新登录。".into())
        })?;
    let account = store.official_account(&account_id)?;
    crate::official_quota::ensure_account_usable(&account)
}

/// 连接 Codex 的调试端口，先写入模型目录，再执行解锁脚本并校验结果。
async fn inject(port: u16, catalog: &[CodexModelInfo]) -> Result<ModelUnlockResult, AppError> {
    let ws_url = codex_page_ws_url(port).await.ok_or_else(|| {
        AppError::Internal("Codex 调试端口已失效，请重新打开 Codex 后再试。".into())
    })?;
    let catalog_json = serde_json::to_string(&json!({ "models": catalog }))
        .map_err(|error| AppError::Internal(error.to_string()))?;
    let set_catalog = format!("window.__CODEX_TOOLS_MODEL_CATALOG__ = {catalog_json}; true");
    evaluate(&ws_url, &set_catalog).await?;
    let result = evaluate(&ws_url, UNLOCK_SCRIPT).await?;
    let status = result
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("failed");
    let injected = evaluate(&ws_url, "window.__CODEX_TOOLS_MODEL_UNLOCKED__ === true")
        .await?
        .as_bool()
        .unwrap_or(false);
    let message = match status {
        "already" => "模型列表已解锁（重复注入，目录已刷新）。".to_string(),
        "ok" => "模型列表已解锁。".to_string(),
        "no_models" => "没有可解锁的模型，请先添加 API 服务。".to_string(),
        _ => "注入完成，但状态未知，请刷新查看。".to_string(),
    };
    Ok(ModelUnlockResult {
        port,
        injected,
        model_count: catalog.len(),
        message,
    })
}

/// 扫描候选端口，返回 Codex 桌面应用页面目标（`app://-`）的调试 WebSocket
/// 地址；没有匹配时返回 `None`。
async fn codex_page_ws_url(port: u16) -> Option<String> {
    probe_port(port).await
}

/// 在所有候选端口上查找 Codex 的 CDP 端点。
async fn find_codex_debug_port_on(ports: &[u16]) -> Option<u16> {
    for &port in ports {
        if probe_port(port).await.is_some() {
            return Some(port);
        }
    }
    None
}

#[derive(Debug, Deserialize)]
struct CdpTarget {
    #[serde(rename = "type")]
    target_type: String,
    #[serde(default)]
    url: String,
    #[serde(default, rename = "webSocketDebuggerUrl")]
    web_socket_debugger_url: Option<String>,
}

fn is_codex_page_target(target: &CdpTarget) -> bool {
    target.target_type == "page"
        && target.url.trim_start().starts_with("app://-")
        && target
            .web_socket_debugger_url
            .as_deref()
            .is_some_and(|url| url.starts_with("ws://"))
}

/// 从目标列表中挑选要注入的页面：优先主窗口（`app://-/index.html`，无查询
/// 参数），避免注入到头像浮层等弹窗页面；没有主窗口时才回退到第一个页面。
fn pick_codex_page_ws(targets: Vec<CdpTarget>) -> Option<String> {
    let pages = targets
        .into_iter()
        .filter(is_codex_page_target)
        .collect::<Vec<_>>();
    if pages.is_empty() {
        return None;
    }
    pages
        .iter()
        .find(|target| !target.url.contains('?'))
        .or_else(|| pages.first())
        .and_then(|target| target.web_socket_debugger_url.clone())
}

/// 对单个端口发起 `GET /json` 探测，返回 Codex 页面目标的 WebSocket 地址。
async fn probe_port(port: u16) -> Option<String> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let mut stream = tokio::time::timeout_at(deadline.into(), TcpStream::connect(address))
        .await
        .ok()?
        .ok()?;
    let request =
        format!("GET /json HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    tokio::time::timeout_at(deadline.into(), stream.write_all(request.as_bytes()))
        .await
        .ok()?
        .ok()?;
    let mut body = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(read)) => {
                body.extend_from_slice(&chunk[..read]);
                if body.len() > MAX_CDP_PROBE_BYTES {
                    break;
                }
            }
            _ => break,
        }
    }
    let response = String::from_utf8_lossy(&body);
    let header_end = response.find("\r\n\r\n")?;
    if !response[..header_end].contains(" 200 ") {
        return None;
    }
    let targets: Vec<CdpTarget> = serde_json::from_str(response[header_end + 4..].trim()).ok()?;
    pick_codex_page_ws(targets)
}

/// 通过 CDP WebSocket 在 Codex 渲染进程执行表达式，返回求值结果
/// （`returnByValue` + `awaitPromise`）。
async fn evaluate(ws_url: &str, expression: &str) -> Result<Value, AppError> {
    let (mut socket, _response) =
        tokio::time::timeout(EVALUATE_TIMEOUT, tokio_tungstenite::connect_async(ws_url))
            .await
            .map_err(|_| AppError::Internal("连接 Codex 调试端口超时。".into()))?
            .map_err(|error| AppError::Internal(format!("无法连接 Codex 调试端口：{error}")))?;
    let request = json!({
        "id": 1,
        "method": "Runtime.evaluate",
        "params": {
            "expression": expression,
            "returnByValue": true,
            "awaitPromise": true,
        }
    });
    tokio::time::timeout(
        EVALUATE_TIMEOUT,
        socket.send(Message::Text(request.to_string())),
    )
    .await
    .map_err(|_| AppError::Internal("向 Codex 发送调试指令超时。".into()))?
    .map_err(|error| AppError::Internal(format!("无法向 Codex 发送调试指令：{error}")))?;
    tokio::time::timeout(EVALUATE_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| AppError::Internal("Codex 调试端口连接中断。".into()))?
                .map_err(|error| AppError::Internal(format!("Codex 调试端口连接错误：{error}")))?;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text)
                .map_err(|error| AppError::Internal(format!("Codex 调试响应格式无效：{error}")))?;
            if value.get("id").and_then(Value::as_u64) != Some(1) {
                continue;
            }
            if let Some(details) = value.pointer("/result/exceptionDetails") {
                let text = details
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("未知错误");
                return Err(AppError::Internal(format!(
                    "Codex 渲染进程执行脚本失败：{text}"
                )));
            }
            return Ok(value
                .pointer("/result/result/value")
                .cloned()
                .unwrap_or(Value::Null));
        }
    })
    .await
    .map_err(|_| AppError::Internal("等待 Codex 响应超时。".into()))?
}

/// 注入到 Codex 渲染进程的解锁脚本。
///
/// - 从 `window.__CODEX_TOOLS_MODEL_CATALOG__` 读取模型目录；
/// - 修补 `Response.prototype.json`，给所有模型列表响应补上白名单外的模型、
///   并取消隐藏被锁的模型；
/// - 修补 Statsig 模型白名单动态配置（`107580212`）的 `available_models`；
/// - 幂等：重复注入只刷新目录，不重复包裹补丁。
const UNLOCK_SCRIPT: &str = r#"(function () {
  function currentModels() {
    var catalog = window.__CODEX_TOOLS_MODEL_CATALOG__ || { models: [] };
    var entries = Array.isArray(catalog.models) ? catalog.models : [];
    var names = [];
    for (var i = 0; i < entries.length; i++) {
      var slug = entries[i] && typeof entries[i].slug === "string" ? entries[i].slug : "";
      if (slug && names.indexOf(slug) === -1) names.push(slug);
    }
    return { entries: entries, names: names };
  }
  function metaFor(slug, entries) {
    for (var i = 0; i < entries.length; i++) if (entries[i] && entries[i].slug === slug) return entries[i];
    return null;
  }
  function descriptor(slug, entries) {
    var meta = metaFor(slug, entries) || {};
    var levels = meta.supported_reasoning_levels || meta.supportedReasoningLevels || [];
    var supported;
    if (Array.isArray(levels) && levels.length) {
      supported = levels.map(function (l) { return { reasoningEffort: l.effort, description: l.description || l.effort }; });
    } else {
      supported = ["low", "medium", "high"].map(function (e) { return { reasoningEffort: e, description: e + " effort" }; });
    }
    return {
      id: slug, model: slug, slug: slug, name: slug,
      displayName: meta.display_name || meta.displayName || slug,
      description: meta.description || meta.description || "Codex Tools 解锁模型",
      hidden: false, isDefault: false,
      defaultReasoningEffort: meta.default_reasoning_level || "medium",
      supportedReasoningEfforts: supported,
      input_modalities: ["text", "image"], supports_personality: false,
      service_tiers: [], default_service_tier: null, additional_speed_tiers: [],
      model_specialty: null
    };
  }
  function patchArray(list) {
    if (!Array.isArray(list)) return false;
    var models = currentModels();
    if (!models.names.length) return false;
    var changed = false;
    var seen = {};
    for (var i = 0; i < list.length; i++) {
      var item = list[i];
      if (!item || typeof item !== "object") continue;
      if (typeof item.model === "string" && item.model) seen[item.model] = true;
      if (models.names.indexOf(item.model) !== -1) {
        if (item.hidden !== false) { item.hidden = false; changed = true; }
        var meta = metaFor(item.model, models.entries);
        if (meta && meta.display_name && item.displayName !== meta.display_name) { item.displayName = meta.display_name; changed = true; }
      }
    }
    for (var j = 0; j < models.names.length; j++) {
      if (!seen[models.names[j]]) { list.push(descriptor(models.names[j], models.entries)); changed = true; }
    }
    return changed;
  }
  function patchContainer(value) {
    if (!value || typeof value !== "object") return false;
    var changed = false;
    if (patchArray(value.models)) changed = true;
    if (patchArray(value.data)) changed = true;
    if (patchArray(value.result)) changed = true;
    if (value.result && patchArray(value.result.data)) changed = true;
    if (value.result && patchArray(value.result.models)) changed = true;
    if (value.message && value.message.result && patchArray(value.message.result.data)) changed = true;
    if (value.pages && value.pages[0] && patchArray(value.pages[0].data)) changed = true;
    var models = currentModels();
    if (Array.isArray(value.availableModels)) {
      models.names.forEach(function (n) { if (value.availableModels.indexOf(n) === -1) value.availableModels.push(n); changed = true; });
    }
    if (Array.isArray(value.available_models)) {
      models.names.forEach(function (n) { if (value.available_models.indexOf(n) === -1) value.available_models.push(n); changed = true; });
    }
    for (var listKey of ["hiddenModels", "hidden_models"]) {
      if (Array.isArray(value[listKey])) {
        var before = value[listKey].length;
        value[listKey] = value[listKey].filter(function (n) { return models.names.indexOf(n) === -1; });
        if (value[listKey].length !== before) changed = true;
      }
    }
    return changed;
  }
  // ---- Statsig：setter hook + 客户端补丁 ----
  function patchStatsigModelDynamicConfig(config) {
    var models = currentModels();
    var value = config && config.value;
    if (!models.names.length || !value || typeof value !== "object") return config;
    var available = Array.isArray(value.available_models) ? value.available_models.slice() : [];
    var changed = false;
    models.names.forEach(function (n) { if (available.indexOf(n) === -1) { available.push(n); changed = true; } });
    var next = {};
    for (var key in value) if (Object.prototype.hasOwnProperty.call(value, key)) next[key] = value[key];
    next.available_models = available;
    if (!changed && next.default_model === value.default_model) return config;
    next.default_model = models.names[0];
    try { config.value = next; } catch (e) { return Object.assign({}, config, { value: next }); }
    return config;
  }
  function patchStatsigClient(client) {
    if (!client || typeof client.getDynamicConfig !== "function") return;
    if (!client.__CODEX_TOOLS_MODEL_WHITELIST_PATCHED__) {
      var originalGet = client.getDynamicConfig.bind(client);
      client.getDynamicConfig = function (name, options) {
        var config = originalGet(name, options);
        if (String(name) === "107580212") return patchStatsigModelDynamicConfig(config);
        return config;
      };
      client.__CODEX_TOOLS_MODEL_WHITELIST_PATCHED__ = true;
    }
    try { patchStatsigModelDynamicConfig(client.getDynamicConfig("107580212", { disableExposureLog: true })); } catch (e) {}
  }
  function statsigClients() {
    var root = window.__STATSIG__ || globalThis.__STATSIG__;
    if (!root || typeof root !== "object") return [];
    var clients = [];
    if (root.firstInstance) clients.push(root.firstInstance);
    if (typeof root.instance === "function") { try { clients.push(root.instance()); } catch (e) {} }
    if (root.instances && typeof root.instances === "object") {
      for (var key in root.instances) if (Object.prototype.hasOwnProperty.call(root.instances, key)) clients.push(root.instances[key]);
    }
    return clients.filter(function (c, i, arr) { return c && typeof c === "object" && arr.indexOf(c) === i; });
  }
  function patchStatsigRoot(root) {
    if (!root || typeof root !== "object") return;
    statsigClients().forEach(patchStatsigClient);
  }
  function installStatsigRootSetter() {
    try {
      var descriptor = Object.getOwnPropertyDescriptor(window, "__STATSIG__");
      if (descriptor && descriptor.configurable === false) return;
      var currentRoot = window.__STATSIG__;
      patchStatsigRoot(currentRoot);
      Object.defineProperty(window, "__STATSIG__", {
        configurable: true,
        get: function () { return currentRoot; },
        set: function (next) {
          currentRoot = next;
          patchStatsigRoot(next);
          statsigClients().forEach(patchStatsigClient);
        }
      });
    } catch (e) {}
  }
  // ---- React Query observer 补丁（绕过 atom 缓存） ----
  function findQueryClientFromFiber() {
    var root = document.getElementById("root");
    if (!root) return null;
    var key = Object.keys(root).find(function (k) { return k.indexOf("__reactContainer") === 0; });
    if (!key) return null;
    var seen = new Set();
    var found = null;
    function visit(fiber, depth) {
      if (!fiber || seen.has(fiber) || depth > 40 || found) return;
      seen.add(fiber);
      for (var fi = 0; fi < 2; fi++) {
        var val = fi === 0 ? fiber.memoizedProps : fiber.memoizedState;
        if (!val || typeof val !== "object") continue;
        var candidates = [val, val.value, val.client];
        for (var ci = 0; ci < candidates.length; ci++) {
          var c = candidates[ci];
          if (c && typeof c.getQueryCache === "function" && typeof c.setQueryData === "function") { found = c; return; }
        }
      }
      visit(fiber.child, depth + 1);
      visit(fiber.sibling, depth + 1);
    }
    visit(root[key], 0);
    return found;
  }
  function patchModelObservers(queryClient) {
    if (!queryClient) return 0;
    var patched = 0;
    var queries = [];
    try { queries = queryClient.getQueryCache().getAll(); } catch (e) { return 0; }
    for (var qi = 0; qi < queries.length; qi++) {
      var q = queries[qi];
      var keyText = JSON.stringify(q.queryKey || []);
      if (keyText.indexOf("models") === -1 || keyText.indexOf("list") === -1) continue;
      var observers = q.observers || [];
      for (var oi = 0; oi < observers.length; oi++) {
        var obs = observers[oi];
        if (!obs || !obs.options || typeof obs.options.select !== "function") continue;
        if (obs.__CODEX_TOOLS_SELECT_PATCHED__) continue;
        var originalSelect = obs.options.select;
        obs.options.select = function (data) {
          var base = originalSelect(data);
          if (!base || typeof base !== "object") return base;
          var models = currentModels();
          var existing = {};
          var list = base.models;
          if (Array.isArray(list)) for (var i = 0; i < list.length; i++) { var m = list[i]; if (m && m.model) existing[m.model] = true; }
          var added = [];
          for (var j = 0; j < models.names.length; j++) {
            if (!existing[models.names[j]]) added.push(descriptor(models.names[j], models.entries));
          }
          if (!added.length) return base;
          return Object.assign({}, base, { models: (list || []).concat(added) });
        };
        obs.__CODEX_TOOLS_SELECT_PATCHED__ = true;
        patched++;
      }
    }
    return patched;
  }
  function refreshQueries(queryClient) {
    if (!queryClient) return;
    try {
      var queries = queryClient.getQueryCache().getAll();
      for (var i = 0; i < queries.length; i++) {
        var keyText = JSON.stringify(queries[i].queryKey || []);
        if (keyText.indexOf("models") === -1 || keyText.indexOf("list") === -1) continue;
        var data = queries[i].state && queries[i].state.data;
        if (data && typeof data === "object") {
          queryClient.setQueryData(queries[i].queryKey, function (old) { return Object.assign({}, old, { __ct: Date.now() }); });
        }
      }
    } catch (e) {}
  }
  // ---- 安装 ----
  installStatsigRootSetter();
  patchStatsigRoot(window.__STATSIG__ || globalThis.__STATSIG__);
  // Response.json 补丁
  if (typeof Response !== "undefined" && Response.prototype && !Response.prototype.json.__CODEX_TOOLS_PATCHED__) {
    var originalJson = Response.prototype.json;
    var wrapped = async function () {
      var payload = await originalJson.apply(this, arguments);
      try { patchContainer(payload); } catch (e) {}
      return payload;
    };
    wrapped.__CODEX_TOOLS_PATCHED__ = true;
    Response.prototype.json = wrapped;
  }
  // 周期重试：等 Statsig SDK 初始化、React Query 就绪
  var startedAt = Date.now();
  var timer = window.setInterval(function () {
    patchStatsigRoot(window.__STATSIG__ || globalThis.__STATSIG__);
    var qc = findQueryClientFromFiber();
    var count = patchModelObservers(qc);
    if (count > 0) refreshQueries(qc);
    if (Date.now() - startedAt > 6000) window.clearInterval(timer);
  }, 100);
  var models = currentModels();
  window.__CODEX_TOOLS_MODEL_UNLOCKED__ = true;
  return { status: models.names.length ? "ok" : "no_models", models: models.names.length };
})()"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ActiveKind, ActiveState, CodexAuthCredential, CodexAuthTokens, OfficialAccountSource,
        ProviderAccountQuota, ProviderApiType, ProviderProfile, QuotaStatus, StoredOfficialAccount,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    fn provider(id: &str, model: &str) -> ProviderProfile {
        ProviderProfile {
            id: id.into(),
            name: "Provider".into(),
            base_url: "https://example.test/v1".into(),
            headers: BTreeMap::new(),
            timeout_secs: 30,
            enabled: true,
            active: false,
            model: model.into(),
            model_context_windows: Default::default(),
            context_window_override: None,
            available_models: if model.trim().is_empty() {
                Vec::new()
            } else {
                vec![model.to_string()]
            },
            selected_models: None,
            custom_models: Default::default(),
            models_dev_meta: Default::default(),
            api_type: ProviderApiType::Responses,
            api_key: Some("secret".into()),
            has_api_key: true,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn provider_with_models(id: &str, model: &str, models: &[&str]) -> ProviderProfile {
        let mut provider = provider(id, model);
        provider.available_models = models.iter().map(|slug| slug.to_string()).collect();
        provider
    }

    #[test]
    fn debug_launch_preflight_rejects_an_active_deactivated_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let saved = store
            .save_official_account(&StoredOfficialAccount {
                id: String::new(),
                name: "停用账号".into(),
                remark: String::new(),
                account_id: "workspace-1".into(),
                email: "account@example.test".into(),
                credential: CodexAuthCredential {
                    auth_mode: "chatgpt".into(),
                    openai_api_key: None,
                    tokens: CodexAuthTokens {
                        id_token: String::new(),
                        access_token: "access-secret".into(),
                        refresh_token: String::new(),
                        account_id: "workspace-1".into(),
                    },
                    last_refresh: "2026-08-20T00:00:00Z".into(),
                },
                source: OfficialAccountSource::ProxyImport,
                expires_at: None,
                quota: ProviderAccountQuota {
                    status: QuotaStatus::Unauthorized,
                    error_code: Some(crate::official_quota::DEACTIVATED_WORKSPACE_CODE.into()),
                    ..Default::default()
                },
                created_at: 0,
                updated_at: 0,
            })
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let error = ensure_active_official_account_usable(&store, ActiveKind::Official)
            .expect_err("停用工作区必须在启动 Codex 前被拦截");

        assert!(error.to_string().contains("工作区已停用"));
    }

    #[test]
    fn built_catalog_is_valid_for_codex_cli_schema() {
        let catalog =
            build_model_catalog_with_windows_for_provider(&provider("provider", "deepseek-v4-pro"));
        // 只含当前服务商 `/models` 返回的模型，不含任何内置模型。
        assert_eq!(catalog.len(), 1);
        let deepseek = &catalog[0];
        assert_eq!(deepseek.slug, "deepseek-v4-pro");
        // CLI 要求 base_instructions 是字符串、supported_reasoning_levels 存在。
        assert!(
            deepseek
                .base_instructions
                .as_deref()
                .is_some_and(|s| !s.is_empty())
        );
        // 提示词不绑定具体模型版本（不写 GPT-5）。
        let instructions = deepseek.base_instructions.as_deref().unwrap();
        assert!(instructions.contains("You are Codex"));
        assert!(!instructions.contains("GPT-5"));
        assert!(
            deepseek
                .supported_reasoning_levels
                .as_ref()
                .is_some_and(|levels| !levels.is_empty())
        );
        assert_eq!(deepseek.visibility.as_deref(), Some("list"));
        assert_eq!(deepseek.context_window, Some(128_000));
        // 序列化后字段名必须与 CLI 一致（snake_case）。
        let value = serde_json::to_value(deepseek).unwrap();
        assert!(value.get("display_name").is_some());
        assert!(value.get("base_instructions").is_some());
        assert!(value.get("supported_reasoning_levels").is_some());
        assert!(value.get("displayName").is_none());
    }

    #[test]
    fn write_model_catalog_persists_file_and_returns_relative_path() {
        let temp = tempfile::tempdir().unwrap();
        let catalog = build_model_catalog_with_windows_for_provider(&provider(
            "provider",
            "deepseek-v4-flash",
        ));

        let relative = write_model_catalog(temp.path(), &catalog).unwrap();

        assert_eq!(
            relative,
            format!("{MODEL_CATALOG_DIR}/{MODEL_CATALOG_FILE}")
        );
        let path = temp.path().join(&relative);
        assert!(path.is_file());
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let models = value["models"].as_array().unwrap();
        assert!(models.iter().any(|m| m["slug"] == "deepseek-v4-flash"));
        // 目录可被 Codex CLI 的 model_catalog_json 解析。
        assert!(models.iter().all(|m| {
            m.get("supported_reasoning_levels").is_some()
                && m.get("base_instructions")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        }));
    }

    #[test]
    fn provider_catalog_contains_only_provider_models() {
        let mut provider = provider_with_models(
            "provider",
            "deepseek-v4-pro",
            &["deepseek-v4-pro", "deepseek-v4-flash"],
        );
        // 手动窗口里即使有与 API 不一致的键（如 "5"），也不能产生模型。
        provider.model_context_windows = BTreeMap::from([
            ("deepseek-chat".into(), 131_072u64),
            ("5".into(), 200_000u64),
        ]);
        provider.models_dev_meta = BTreeMap::from([(
            "deepseek-v4-pro".into(),
            crate::models::ProviderModelsDevMeta {
                name: Some("DeepSeek V4 Pro".into()),
                context_window: Some(1_000_000),
                description: Some("百万 token 上下文旗舰模型".into()),
            },
        )]);

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        let by_slug: BTreeMap<_, _> = catalog
            .iter()
            .map(|model| (model.slug.as_str(), model))
            .collect();

        // 只包含 /models 接口返回的可用模型；窗口键不产生模型。
        assert_eq!(by_slug.len(), 2);
        assert!(!by_slug.contains_key("deepseek-chat"));
        assert!(!by_slug.contains_key("5"));
        // models.dev 元数据精确匹配补充名称/窗口/简介。
        assert_eq!(by_slug["deepseek-v4-pro"].display_name, "DeepSeek V4 Pro");
        assert_eq!(by_slug["deepseek-v4-pro"].context_window, Some(1_000_000));
        assert_eq!(
            by_slug["deepseek-v4-pro"].description.as_deref(),
            Some("百万 token 上下文旗舰模型")
        );
        // 无元数据的模型用 slug、默认窗口、无简介。
        assert_eq!(
            by_slug["deepseek-v4-flash"].display_name,
            "deepseek-v4-flash"
        );
        assert_eq!(by_slug["deepseek-v4-flash"].context_window, Some(128_000));
        assert_eq!(by_slug["deepseek-v4-flash"].description, None);
        // 不包含任何内置 GPT / codex 模型。
        assert!(!by_slug.contains_key("gpt-5.6-sol"));
        assert!(!by_slug.contains_key("codex-latest"));
        assert!(!by_slug.contains_key("o4-mini"));
    }

    #[test]
    fn context_window_keys_never_create_models() {
        // 回归：之前窗口键会被当成模型（如用户手动填写的 "5"），
        // 现在只有 /models 接口返回的 id 才算模型。
        let mut provider = provider_with_models("provider", "", &["deepseek-v4-pro"]);
        provider.model_context_windows = BTreeMap::from([("5".into(), 200_000u64)]);

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        let slugs: Vec<_> = catalog.iter().map(|model| model.slug.as_str()).collect();
        assert_eq!(slugs, vec!["deepseek-v4-pro"]);
    }

    #[test]
    fn provider_catalog_api_model_uses_stored_window() {
        let mut provider =
            provider_with_models("provider", "deepseek-v4-pro", &["deepseek-v4-pro"]);
        provider.model_context_windows = BTreeMap::from([("deepseek-v4-pro".into(), 1_000_000u64)]);

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].context_window, Some(1_000_000));
    }

    #[test]
    fn provider_catalog_context_window_override_wins_for_every_model() {
        let mut provider = provider_with_models("provider", "model-a", &["model-a", "model-b"]);
        provider.model_context_windows =
            BTreeMap::from([("model-a".into(), 128_000), ("model-b".into(), 256_000)]);
        provider.models_dev_meta = BTreeMap::from([(
            "model-b".into(),
            crate::models::ProviderModelsDevMeta {
                context_window: Some(512_000),
                ..Default::default()
            },
        )]);
        provider.context_window_override = Some(1_000_000);

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        assert_eq!(catalog.len(), 2);
        for model in catalog {
            assert_eq!(model.context_window, Some(1_000_000));
            assert_eq!(model.max_context_window, Some(1_000_000));
        }
    }

    #[test]
    fn legacy_default_model_never_creates_a_catalog_entry() {
        let mut provider = provider("provider", "legacy-manual-model");
        provider.available_models.clear();

        assert!(build_model_catalog_with_windows_for_provider(&provider).is_empty());
    }

    #[test]
    fn provider_catalog_filters_selected_models_to_available_ones() {
        let mut provider =
            provider_with_models("provider", "model-a", &["model-a", "model-b", "model-c"]);
        // 未设置选择时使用全部可用模型。
        let slugs = |provider: &ProviderProfile| {
            build_model_catalog_with_windows_for_provider(provider)
                .into_iter()
                .map(|model| model.slug)
                .collect::<Vec<_>>()
        };
        assert_eq!(slugs(&provider), vec!["model-a", "model-b", "model-c"]);

        // 只选择可用模型的子集。
        provider.selected_models = Some(vec!["model-c".into(), "model-a".into()]);
        assert_eq!(slugs(&provider), vec!["model-a", "model-c"]);

        // 选择列表中混入未同步/不可用的模型时应被过滤掉。
        provider.selected_models = Some(vec![
            "model-b".into(),
            "removed-model".into(),
            "model-a".into(),
        ]);
        assert_eq!(slugs(&provider), vec!["model-a", "model-b"]);

        // 显式空选择表示不写入任何模型。
        provider.selected_models = Some(Vec::new());
        assert!(slugs(&provider).is_empty());
    }
    #[test]
    fn custom_models_enter_catalog_without_selection() {
        // 未设置 selected_models 时，有效模型 = available_models ∪ custom_models。
        let mut provider = provider_with_models("provider", "model-a", &["model-a", "model-b"]);
        provider.custom_models = vec!["custom-1".into(), "custom-2".into()];

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        let slugs: Vec<_> = catalog.iter().map(|model| model.slug.as_str()).collect();
        // 目录按 slug 排序（内部 BTreeMap）。
        assert_eq!(slugs, vec!["custom-1", "custom-2", "model-a", "model-b"]);
        // 自定义模型无元数据：展示名用 slug、回退默认窗口。
        let custom = catalog
            .iter()
            .find(|model| model.slug == "custom-1")
            .unwrap();
        assert_eq!(custom.display_name, "custom-1");
        assert_eq!(custom.context_window, Some(128_000));
    }

    #[test]
    fn custom_models_respect_selected_models() {
        // 设置 selected_models 时只保留同时出现在有效集合（available ∪ custom）的项。
        let mut provider = provider_with_models("provider", "model-a", &["model-a", "model-b"]);
        provider.custom_models = vec!["custom-1".into()];
        provider.selected_models =
            Some(vec!["custom-1".into(), "custom-2".into(), "model-b".into()]);

        let catalog = build_model_catalog_with_windows_for_provider(&provider);
        let slugs: Vec<_> = catalog.iter().map(|model| model.slug.as_str()).collect();
        // custom-2 不在有效集合内，被过滤；custom-1 是自定义模型，被保留。
        assert_eq!(slugs, vec!["custom-1", "model-b"]);

        // 显式空选择表示不写入任何模型（含自定义）。
        provider.selected_models = Some(Vec::new());
        assert!(build_model_catalog_with_windows_for_provider(&provider).is_empty());
    }

    #[test]
    fn normalize_custom_models_drops_duplicates_and_blank() {
        // 重复、空白、与 available_models 重复的自定义模型都被规范化掉。
        let mut provider = provider_with_models("provider", "model-a", &["model-a"]);
        provider.custom_models = vec![
            "  custom-1  ".into(),
            "custom-1".into(),
            "".into(),
            "   ".into(),
            "model-a".into(),
        ];

        provider.normalize_and_validate().unwrap();
        assert_eq!(provider.custom_models, vec!["custom-1"]);
    }

    #[test]
    fn model_catalog_uses_only_active_provider() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .connections_save_provider(provider("provider-a", "deepseek-v4-pro"))
            .unwrap();
        store
            .connections_save_provider(provider("provider-b", "kimi-k2"))
            .unwrap();
        store.activate("provider-a").unwrap();

        let catalog = model_catalog(&store).unwrap();
        let slugs: Vec<_> = catalog.iter().map(|model| model.slug.as_str()).collect();
        // 只含激活服务商 provider-a 的模型，不合并其他服务商或内置模型。
        assert_eq!(slugs, vec!["deepseek-v4-pro"]);
        assert!(!slugs.contains(&"kimi-k2"));
        assert!(!slugs.contains(&"gpt-5.6-sol"));
        assert!(!slugs.contains(&"codex-latest"));
    }

    #[tokio::test]
    async fn catalog_writer_rereads_active_after_waiting_for_transaction_locks() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path().join("data")).unwrap());
        store
            .connections_save_provider(provider("provider-a", "model-a"))
            .unwrap();
        store
            .connections_save_provider(provider("provider-b", "model-b"))
            .unwrap();
        store.activate("provider-a").unwrap();
        let activation = Arc::new(ActivationLock::default());

        // 模拟激活事务持有统一锁并切到 B；目录写入口已开始排队，但只能在
        // 激活提交并释放锁后读取，因此不能携带旧的 A 目录继续写入。
        let model_transaction = activation.2.lock().await;
        let activation_guard = activation.0.lock().await;
        let waiting_store = store.clone();
        let waiting_activation = activation.clone();
        let writer = tokio::spawn(async move {
            let context = model_catalog_write_context(&waiting_store, &waiting_activation)
                .await
                .unwrap();
            (
                context.active_kind,
                context
                    .catalog
                    .into_iter()
                    .map(|model| model.slug)
                    .collect::<Vec<_>>(),
            )
        });
        tokio::task::yield_now().await;
        store.activate("provider-b").unwrap();
        drop(activation_guard);
        drop(model_transaction);

        let (kind, models) = writer.await.unwrap();
        assert!(matches!(kind, ActiveKind::Provider));
        assert_eq!(models, vec!["model-b"]);
    }

    #[test]
    fn model_catalog_is_empty_for_official_or_no_active_source() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        // 未激活任何服务。
        assert!(model_catalog(&store).unwrap().is_empty());
        // 官方 OpenAI：不注入默认 GPT 模型。
        store
            .update(|state| {
                state.active = ActiveState {
                    kind: ActiveKind::Official,
                    provider_id: None,
                    account_id: None,
                };
                Ok(())
            })
            .unwrap();
        assert!(model_catalog(&store).unwrap().is_empty());
    }

    #[test]
    fn catalog_serializes_to_valid_snake_case_json() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .connections_save_provider(provider("provider", "deepseek-v4-pro"))
            .unwrap();
        store.activate("provider").unwrap();
        let catalog = model_catalog(&store).unwrap();
        let value = serde_json::to_value(json!({ "models": catalog })).unwrap();
        let first = &value["models"][0];
        assert!(first.get("slug").is_some());
        assert!(first.get("display_name").is_some());
        assert!(first.get("displayName").is_none());
    }

    #[test]
    fn unlock_script_contains_required_patch_markers() {
        assert!(UNLOCK_SCRIPT.contains("Response.prototype.json"));
        assert!(UNLOCK_SCRIPT.contains("107580212"));
        assert!(UNLOCK_SCRIPT.contains("__CODEX_TOOLS_MODEL_UNLOCKED__"));
        assert!(UNLOCK_SCRIPT.contains("available_models"));
        assert!(UNLOCK_SCRIPT.contains("hiddenModels"));
    }

    #[tokio::test]
    async fn probe_detects_codex_page_target() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"[{"id":"page-1","type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://127.0.0.1:PORT/devtools/page/abc"},{"id":"other","type":"other","url":"about:blank"}]"#;
            let body = body.replace("PORT", &port.to_string());
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let ws_url = probe_port(port).await;
        let expected = format!("ws://127.0.0.1:{port}/devtools/page/abc");
        assert_eq!(ws_url.as_deref(), Some(expected.as_str()));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn inject_exchanges_catalog_script_and_verification() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = loop {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            if !PROBE_PORTS.contains(&listener.local_addr().unwrap().port()) {
                break listener;
            }
        };
        let port = listener.local_addr().unwrap().port();
        let expected_ws_url = format!("ws://127.0.0.1:{port}/devtools/page/abc");

        let server = tokio::spawn(async move {
            // 1) /json 探测：返回 Codex 页面目标。
            let (mut probe, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0_u8; 4096];
            loop {
                let read = probe.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let body = format!(
                r#"[{{"id":"page-1","type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"{expected_ws_url}"}}]"#
            );
            probe
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            drop(probe);
            // 2) WebSocket 会话：每个 evaluate 使用一条独立连接，
            //    与真实 CDP 行为一致；逐条回应结果。
            let mut received = Vec::new();
            loop {
                let (ws_stream, _) = listener.accept().await.expect("server: accept ws failed");
                let mut socket = accept_async(ws_stream)
                    .await
                    .expect("server: handshake failed");
                if let Some(Ok(Message::Text(text))) = socket.next().await {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    let expression = value
                        .pointer("/params/expression")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    received.push(expression.to_string());
                    let reply = if expression == "window.__CODEX_TOOLS_MODEL_UNLOCKED__ === true"
                        || expression.starts_with("window.__CODEX_TOOLS_MODEL_CATALOG__ =")
                    {
                        json!({"id": 1, "result": {"result": {"type": "boolean", "value": true}}})
                    } else {
                        json!({
                            "id": 1,
                            "result": {"result": {"type": "object", "value": {"status": "ok", "models": 3}}}
                        })
                    };
                    socket.send(Message::Text(reply.to_string())).await.unwrap();
                }
                if received.len() >= 3 {
                    break;
                }
            }
            received
        });

        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .connections_save_provider(provider("provider", "deepseek-v4-pro"))
            .unwrap();
        store.activate("provider").unwrap();
        let catalog = model_catalog(&store).unwrap();
        let result = inject(port, &catalog).await.unwrap();

        assert!(result.injected);
        assert_eq!(result.port, port);
        assert!(result.message.contains("解锁"));

        let received = server.await.unwrap();
        assert_eq!(received.len(), 3);
        assert!(received[0].contains("__CODEX_TOOLS_MODEL_CATALOG__"));
        assert!(received[0].contains("deepseek-v4-pro"));
        assert!(received[1].contains("Response.prototype.json"));
        assert_eq!(
            received[2],
            "window.__CODEX_TOOLS_MODEL_UNLOCKED__ === true"
        );
    }

    #[tokio::test]
    async fn probe_prefers_main_window_over_popup_targets() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            // 弹窗目标排在最前，主窗口排在后面：必须选中主窗口。
            let body = format!(
                r#"[{{"id":"popup","type":"page","url":"app://-/index.html?initialRoute=%2Favatar-overlay","webSocketDebuggerUrl":"ws://127.0.0.1:{port}/devtools/page/popup"}},{{"id":"main","type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://127.0.0.1:{port}/devtools/page/main"}}]"#
            );
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let expected = format!("ws://127.0.0.1:{port}/devtools/page/main");
        assert_eq!(probe_port(port).await.as_deref(), Some(expected.as_str()));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn probe_ignores_non_codex_targets_and_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"[{"id":"web","type":"page","url":"https://example.test/","webSocketDebuggerUrl":"ws://127.0.0.1/devtools/page/abc"}]"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        assert!(probe_port(port).await.is_none());
        server.join().unwrap();
        assert!(probe_port(1).await.is_none());
    }

    #[tokio::test]
    async fn status_reports_running_without_debug_port() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .connections_save_provider(provider("provider", "deepseek-v4-pro"))
            .unwrap();
        store.activate("provider").unwrap();

        let status = status(&store).await.unwrap();

        // 不依赖真实安装：只校验结构与目录内容的一致性，避免被环境中的
        // Codex 调试实例干扰。
        assert_eq!(status.models, vec!["deepseek-v4-pro"]);
        assert_eq!(status.model_count, status.models.len());
    }

    #[test]
    fn unlock_without_debug_port_returns_actionable_error() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .connections_save_provider(provider("provider", "deepseek-v4-pro"))
            .unwrap();
        store.activate("provider").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // 端口 1 通常不可连接，用来验证“未找到调试端口”的错误提示。
        let result = runtime.block_on(unlock_on(&store, &ActivationLock::default(), &[1]));

        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("调试端口"));
    }
}
