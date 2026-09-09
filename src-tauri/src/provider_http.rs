use crate::models::{AppError, ProviderProfile};

pub(crate) const MAX_UPSTREAM_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_FETCHED_MODELS: usize = 10_000;
const MAX_MODEL_ID_BYTES: usize = 512;
const MAX_MODEL_DESCRIPTION_BYTES: usize = 16 * 1024;

pub(crate) async fn read_response_body_limited(
    response: reqwest::Response,
    limit: usize,
) -> Result<bytes::Bytes, AppError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(AppError::InvalidConfig(format!(
            "上游响应体超过允许的 {limit} 字节。"
        )));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        let chunk = chunk
            .map_err(|error| AppError::InvalidConfig(format!("读取上游响应体失败：{error}")))?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(AppError::InvalidConfig(format!(
                "上游响应体超过允许的 {limit} 字节。"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.into())
}

/// 从服务 `/models` 接口解析出的单个模型及其可选信息。
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedModelDetail {
    pub id: String,
    /// 服务方返回的上下文窗口（token）；没有返回时为空。
    pub context_window: Option<u64>,
    /// 服务方返回的模型简介；没有返回时为空。
    pub description: Option<String>,
}

/// 根据 base_url 是否已带版本路径（/v1、/openai/v2）拼接上游接口路径。
/// 应用侧直连与转换代理共用这一实现，避免两处版本探测逻辑漂移。
pub fn endpoint_for(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.rsplit('/').next().is_some_and(|part| {
        part.starts_with('v') && part[1..].chars().all(|value| value.is_ascii_digit())
    }) {
        format!("{base}/{path}")
    } else {
        format!("{base}/v1/{path}")
    }
}

pub fn models_endpoint(base_url: &str) -> String {
    endpoint_for(base_url, "models")
}

pub fn provider_test_succeeded(status: reqwest::StatusCode) -> bool {
    status.is_success()
}

/// 获取服务 `/models` 接口的模型列表，并尽量解析每个模型的上下文窗口。
/// 上下文窗口字段名在不同服务间差异很大，这里兼容常见命名：
/// `context_window`、`context_length`、`max_context_length`、
/// `max_model_len`、`max_input_tokens` 以及嵌套的 `limits.context_tokens`。
pub async fn fetch_model_details(
    client: &reqwest::Client,
    provider: &ProviderProfile,
) -> Result<Vec<FetchedModelDetail>, AppError> {
    let endpoint = models_endpoint(&provider.base_url);
    let key = provider.api_key.as_deref().unwrap_or_default();
    let mut request = client.get(&endpoint).headers(custom_headers(provider)?);
    if !key.is_empty() {
        request = request.bearer_auth(key);
    }
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(provider.timeout_secs.max(1)),
        request.send(),
    )
    .await
    .map_err(|_| AppError::InvalidConfig("获取模型列表超时，请检查网络后重试。".into()))?
    .map_err(|error| AppError::InvalidConfig(format!("无法连接到服务获取模型列表：{error}")))?;
    if !response.status().is_success() {
        return Err(AppError::InvalidConfig(format!(
            "模型列表接口返回 HTTP {}，请确认服务支持 OpenAI /models 接口。",
            response.status().as_u16()
        )));
    }
    let body = read_response_body_limited(response, MAX_UPSTREAM_BODY_BYTES)
        .await
        .map_err(|_| AppError::InvalidConfig("模型列表响应过大或读取失败。".into()))?;
    let payload: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|_| AppError::InvalidConfig("模型列表响应不是有效 JSON。".into()))?;
    parse_model_details_payload(&payload)
}

fn parse_model_details_payload(
    payload: &serde_json::Value,
) -> Result<Vec<FetchedModelDetail>, AppError> {
    let items = payload
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| AppError::InvalidConfig("模型列表响应缺少 data 数组。".into()))?;
    if items.len() > MAX_FETCHED_MODELS {
        return Err(AppError::InvalidConfig(format!(
            "模型列表数量超过允许的 {MAX_FETCHED_MODELS} 个。"
        )));
    }
    if items.iter().any(|item| {
        item.get("id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id.trim().len() > MAX_MODEL_ID_BYTES)
            || model_description_value(item)
                .is_some_and(|description| description.trim().len() > MAX_MODEL_DESCRIPTION_BYTES)
    }) {
        return Err(AppError::InvalidConfig(
            "模型 ID 或简介超过允许长度。".into(),
        ));
    }
    let mut models = items
        .iter()
        .filter_map(|item| {
            let id = item
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .filter(|id| id.len() <= MAX_MODEL_ID_BYTES)
                .map(str::to_owned)?;
            Some(FetchedModelDetail {
                id,
                context_window: parse_model_context_window(item),
                description: parse_model_description(item),
            })
        })
        .collect::<Vec<_>>();
    models.sort_by(|left, right| left.id.cmp(&right.id));
    models.dedup_by(|left, right| left.id == right.id);
    if models.is_empty() {
        return Err(AppError::InvalidConfig(
            "服务没有返回可用的模型列表。".into(),
        ));
    }
    Ok(models)
}

/// 从 `/models` 单项里解析模型简介；优先 `description` 字段，
/// 兼容少量嵌套位置；空白视为没有。
fn parse_model_description(item: &serde_json::Value) -> Option<String> {
    model_description_value(item).and_then(|text| {
        let text = text.trim();
        (!text.is_empty() && text.len() <= MAX_MODEL_DESCRIPTION_BYTES).then(|| text.to_owned())
    })
}

fn model_description_value(item: &serde_json::Value) -> Option<&str> {
    const PATHS: &[&[&str]] = &[
        &["description"],
        &["meta", "description"],
        &["info", "description"],
        &["model_info", "description"],
    ];
    for path in PATHS {
        let value = path.iter().fold(item, |current, key| {
            current.get(key).unwrap_or(&serde_json::Value::Null)
        });
        if let Some(text) = value.as_str().filter(|text| !text.trim().is_empty()) {
            return Some(text);
        }
    }
    None
}

/// 从 `/models` 单项里解析上下文窗口；识别常见字段名与嵌套位置。
fn parse_model_context_window(item: &serde_json::Value) -> Option<u64> {
    const TOP_LEVEL_KEYS: &[&str] = &[
        "context_window",
        "context_length",
        "max_context_length",
        "max_context_tokens",
        "max_model_len",
        "max_input_tokens",
        "context_tokens",
        "max_tokens",
    ];
    for key in TOP_LEVEL_KEYS {
        if let Some(value) = parse_window_number(item.get(key)) {
            return Some(value);
        }
    }
    const NESTED_PATHS: &[&[&str]] = &[
        &["limits", "context_tokens"],
        &["token_limits", "context_tokens"],
        &["model_config", "context_window"],
        &["meta", "context_window"],
        &["meta", "max_context_length"],
        &["parameters", "context_window"],
    ];
    for path in NESTED_PATHS {
        if let Some(value) = nested_window_number(item, path) {
            return Some(value);
        }
    }
    None
}

fn nested_window_number(item: &serde_json::Value, path: &[&str]) -> Option<u64> {
    let mut current = item;
    for key in path {
        current = current.get(*key)?;
        if current.is_null() {
            return None;
        }
    }
    parse_window_number(Some(current))
}

/// 把数字或数字字符串解析为上下文窗口；0 或异常值视为无效。
fn parse_window_number(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    let parsed = value.as_u64().or_else(|| {
        value
            .as_str()
            .and_then(|text| text.trim().parse::<u64>().ok())
    });
    parsed.filter(|window| *window > 0 && *window < 1 << 50)
}

pub fn headers_from_pairs(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> Result<reqwest::header::HeaderMap, AppError> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in pairs {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| AppError::InvalidConfig(format!("请求头名称无效：{name}")))?;
        if name == reqwest::header::USER_AGENT {
            continue;
        }
        let value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| AppError::InvalidConfig("请求头内容包含无效字符。".into()))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

pub fn custom_headers(provider: &ProviderProfile) -> Result<reqwest::header::HeaderMap, AppError> {
    headers_from_pairs(provider.headers.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderApiType;
    use serde_json::json;

    #[test]
    fn parses_context_window_from_common_field_names() {
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "context_window": 131072})),
            Some(131072)
        );
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "context_length": 200_000})),
            Some(200_000)
        );
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "max_model_len": "128000"})),
            Some(128_000)
        );
        assert_eq!(
            parse_model_context_window(&json!({
                "id": "m",
                "limits": {"context_tokens": 1000000}
            })),
            Some(1_000_000)
        );
        assert_eq!(
            parse_model_context_window(&json!({
                "id": "m",
                "token_limits": {"context_tokens": 131072}
            })),
            Some(131_072)
        );
    }

    #[test]
    fn missing_or_invalid_windows_fall_back_to_none() {
        assert_eq!(parse_model_context_window(&json!({"id": "m"})), None);
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "context_window": 0})),
            None
        );
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "context_length": "unknown"})),
            None
        );
        assert_eq!(
            parse_model_context_window(&json!({"id": "m", "limits": {"context_tokens": null}})),
            None
        );
    }

    #[test]
    fn parses_description_from_common_field_names() {
        assert_eq!(
            parse_model_description(&json!({"id": "m", "description": "高效编码模型"})),
            Some("高效编码模型".into())
        );
        assert_eq!(
            parse_model_description(&json!({"id": "m", "meta": {"description": "嵌套简介"}})),
            Some("嵌套简介".into())
        );
        assert_eq!(
            parse_model_description(&json!({"id": "m", "description": "  "})),
            None
        );
        assert_eq!(parse_model_description(&json!({"id": "m"})), None);
        assert_eq!(
            parse_model_description(&json!({"id": "m", "description": 42})),
            None
        );
    }

    #[tokio::test]
    async fn fetch_model_details_captures_windows_and_deduplicates() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            let body = r#"{"data":[
                {"id":"claude-sonnet","context_window":200000,"description":"Claude Sonnet"},
                {"id":"deepseek-v4-flash"},
                {"id":"deepseek-v4-flash"}
            ]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let mut provider = provider("p");
        provider.base_url = format!("http://{address}/v1");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let details = fetch_model_details(&client, &provider).await.unwrap();

        assert_eq!(details.len(), 2);
        assert_eq!(details[0].id, "claude-sonnet");
        assert_eq!(details[0].context_window, Some(200_000));
        assert_eq!(details[0].description.as_deref(), Some("Claude Sonnet"));
        assert_eq!(details[1].id, "deepseek-v4-flash");
        assert_eq!(details[1].context_window, None);
        assert_eq!(details[1].description, None);
        server.join().unwrap();
    }

    #[test]
    fn provider_test_only_accepts_success_responses() {
        assert!(provider_test_succeeded(reqwest::StatusCode::OK));
        assert!(provider_test_succeeded(reqwest::StatusCode::NO_CONTENT));
        assert!(!provider_test_succeeded(reqwest::StatusCode::FOUND));
        assert!(!provider_test_succeeded(reqwest::StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn model_endpoint_preserves_versioned_api_roots() {
        assert_eq!(
            models_endpoint("https://api.example.test/v1/"),
            "https://api.example.test/v1/models"
        );
        assert_eq!(
            models_endpoint("https://api.example.test/openai/v2"),
            "https://api.example.test/openai/v2/models"
        );
        assert_eq!(
            models_endpoint("https://api.example.test/openai"),
            "https://api.example.test/openai/v1/models"
        );
    }

    fn provider(id: &str) -> ProviderProfile {
        ProviderProfile {
            id: id.into(),
            name: id.into(),
            base_url: "http://127.0.0.1:9/v1".into(),
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
            api_type: ProviderApiType::Responses,
            api_key: Some("secret".into()),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn fetch_models_parses_ids_sorted_and_deduplicated() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            let body =
                r#"{"data":[{"id":"gpt-5.6-luna"},{"id":"claude-sonnet"},{"id":"gpt-5.6-luna"}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let mut provider = provider("p");
        provider.base_url = format!("http://{address}/v1");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let models = fetch_model_details(&client, &provider).await.unwrap();

        assert_eq!(
            models
                .into_iter()
                .map(|detail| detail.id)
                .collect::<Vec<_>>(),
            vec!["claude-sonnet".to_string(), "gpt-5.6-luna".to_string()]
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn fetch_models_rejects_empty_or_failed_responses() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{{\"data\":[]}}",
                11,
            )
            .unwrap();
        });
        let mut provider = provider("p");
        provider.base_url = format!("http://{address}/v1");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let error = fetch_model_details(&client, &provider).await.unwrap_err();
        assert!(error.to_string().contains("没有返回可用的模型列表"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn fetch_models_reads_chunked_response_without_content_length() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            let chunks = [r#"{"data":[{"id":"chunk"#, r#"ed-model"}]}"#];
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
                .unwrap();
            for chunk in chunks {
                write!(stream, "{:X}\r\n{}\r\n", chunk.len(), chunk).unwrap();
            }
            stream.write_all(b"0\r\n\r\n").unwrap();
        });
        let mut provider = provider("p");
        provider.base_url = format!("http://{address}/v1");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let models = fetch_model_details(&client, &provider).await.unwrap();
        assert_eq!(models[0].id, "chunked-model");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn fetch_models_rejects_chunked_body_over_limit() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
                .unwrap();
            let chunk = vec![b'x'; MAX_UPSTREAM_BODY_BYTES + 1];
            write!(stream, "{:X}\r\n", chunk.len()).unwrap();
            stream.write_all(&chunk).unwrap();
            stream.write_all(b"\r\n0\r\n\r\n").unwrap();
        });
        let mut provider = provider("p");
        provider.base_url = format!("http://{address}/v1");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let error = fetch_model_details(&client, &provider).await.unwrap_err();
        assert!(error.to_string().contains("过大"));
        server.join().unwrap();
    }

    #[test]
    fn model_payload_rejects_excessive_count_and_field_lengths() {
        let too_many = json!({
            "data": (0..=MAX_FETCHED_MODELS)
                .map(|index| json!({"id": format!("model-{index}")}))
                .collect::<Vec<_>>()
        });
        assert!(
            parse_model_details_payload(&too_many)
                .unwrap_err()
                .to_string()
                .contains("数量")
        );

        for payload in [
            json!({"data": [{"id": "x".repeat(MAX_MODEL_ID_BYTES + 1)}]}),
            json!({"data": [{
                "id": "model",
                "description": "x".repeat(MAX_MODEL_DESCRIPTION_BYTES + 1)
            }]}),
        ] {
            assert!(
                parse_model_details_payload(&payload)
                    .unwrap_err()
                    .to_string()
                    .contains("超过允许长度")
            );
        }
    }
}
