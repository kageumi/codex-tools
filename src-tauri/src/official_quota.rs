use crate::models::{
    AppError, OfficialAccountSource, QuotaData, QuotaStatus, QuotaWindow, ResetCreditDetailsStatus,
    ResetCreditSummary, StoredOfficialAccount,
};
use futures_util::StreamExt;
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, REFERER, RETRY_AFTER, USER_AGENT,
};
use serde_json::Value;
use std::time::Duration;

const OPENAI_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const MAX_QUOTA_RESPONSE_BYTES: usize = 256 * 1024;
pub(crate) const DEACTIVATED_WORKSPACE_CODE: &str = "deactivated_workspace";

#[derive(Debug)]
pub struct QuotaFetchError {
    pub status: QuotaStatus,
    pub message: String,
    pub code: Option<String>,
    retryable: bool,
}

impl QuotaFetchError {
    pub(crate) fn new(status: QuotaStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code: None,
            retryable: false,
        }
    }

    pub(crate) fn retryable(message: impl Into<String>) -> Self {
        Self {
            status: QuotaStatus::Error,
            message: message.into(),
            code: None,
            retryable: true,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

pub(crate) fn account_workspace_is_deactivated(account: &StoredOfficialAccount) -> bool {
    account.quota.error_code.as_deref() == Some(DEACTIVATED_WORKSPACE_CODE)
}

pub(crate) fn ensure_account_usable(account: &StoredOfficialAccount) -> Result<(), AppError> {
    if account_workspace_is_deactivated(account) {
        return Err(AppError::InvalidConfig(format!(
            "账号所属工作区已停用（{DEACTIVATED_WORKSPACE_CODE}，HTTP 402），无法设为当前账号或启动 Codex，请更换账号或联系工作区管理员。"
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub struct QuotaFetchResult {
    pub data: QuotaData,
    pub plan_type: Option<String>,
    pub reset_credits: ResetCreditSummary,
}

pub async fn fetch_quota(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
) -> Result<QuotaFetchResult, QuotaFetchError> {
    fetch_quota_from(client, account, OPENAI_USAGE_URL).await
}

pub(crate) fn official_headers(
    account: &StoredOfficialAccount,
) -> Result<HeaderMap, QuotaFetchError> {
    let access_token = account.credential.tokens.access_token.trim();
    if access_token.is_empty() {
        return Err(QuotaFetchError::new(
            QuotaStatus::Unauthorized,
            "账号缺少 Access Token，请重新进行官方授权或导入 Cookie 数据",
        ));
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {access_token}"))
            .map_err(|_| QuotaFetchError::new(QuotaStatus::Error, "Access Token 包含无效字符"))?,
    );
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(REFERER, HeaderValue::from_static("https://chatgpt.com/"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(crate::network::codex_user_agent())
            .expect("Codex CLI User-Agent 应始终是有效请求头"),
    );
    headers.insert("OpenAI-Beta", HeaderValue::from_static("codex-1"));
    headers.insert("oai-language", HeaderValue::from_static("zh-CN"));
    headers.insert("originator", HeaderValue::from_static("Codex Desktop"));

    if should_send_account_id(account) {
        headers.insert(
            "ChatGPT-Account-Id",
            HeaderValue::from_str(account.account_id.trim())
                .map_err(|_| QuotaFetchError::new(QuotaStatus::Error, "账号标识包含无效字符"))?,
        );
    }

    Ok(headers)
}

async fn fetch_quota_from(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
    endpoint: &str,
) -> Result<QuotaFetchResult, QuotaFetchError> {
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        client
            .get(endpoint)
            .headers(official_headers(account)?)
            .send(),
    )
    .await
    .map_err(|_| QuotaFetchError::retryable("OpenAI 额度查询超时，请稍后重试"))?
    .map_err(|error| {
        if error.is_connect() || error.is_timeout() {
            QuotaFetchError::retryable("无法连接 OpenAI 额度服务，请检查网络或系统代理")
        } else {
            QuotaFetchError::new(QuotaStatus::Error, "无法完成 OpenAI 额度请求")
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 64)
            .map(str::to_owned);
        let payload = read_optional_error_json(response).await;
        return Err(classify_http_error(
            status,
            payload.as_ref(),
            retry_after.as_deref(),
        ));
    }
    let payload = read_bounded_json(response).await?;
    let data =
        parse_quota_payload(&payload, chrono::Utc::now().timestamp()).map_err(
            |error| match error {
                QuotaParseError::Unsupported(message) => {
                    QuotaFetchError::new(QuotaStatus::Unsupported, message)
                }
                QuotaParseError::Invalid(message) => {
                    QuotaFetchError::new(QuotaStatus::Error, message)
                }
            },
        )?;
    Ok(QuotaFetchResult {
        data,
        plan_type: parse_plan_type(&payload),
        reset_credits: parse_reset_credit_summary(&payload),
    })
}

pub(crate) fn parse_reset_credit_summary(payload: &Value) -> ResetCreditSummary {
    let credits = payload
        .get("rate_limit_reset_credits")
        .or_else(|| payload.get("rateLimitResetCredits"))
        .or_else(|| payload.pointer("/data/rate_limit_reset_credits"))
        .or_else(|| payload.pointer("/data/rateLimitResetCredits"))
        .or_else(|| {
            (payload.get("available_count").is_some()
                || payload.get("availableCount").is_some()
                || payload.get("credits").is_some())
            .then_some(payload)
        });
    let Some(credits) = credits.filter(|value| value.is_object()) else {
        return ResetCreditSummary::default();
    };
    let available_count = credits
        .get("available_count")
        .or_else(|| credits.get("availableCount"))
        .and_then(parse_number)
        .and_then(|value| u32::try_from(value).ok());
    let detail_status = match credits
        .get("credits")
        .filter(|value| value.is_array())
        .and_then(Value::as_array)
    {
        Some(items)
            if available_count.is_some_and(|count| {
                items
                    .iter()
                    .filter(|item| item.get("status").and_then(Value::as_str) == Some("available"))
                    .count()
                    >= count as usize
            }) =>
        {
            ResetCreditDetailsStatus::Complete
        }
        Some(_) => ResetCreditDetailsStatus::Partial,
        None => ResetCreditDetailsStatus::Unknown,
    };
    ResetCreditSummary {
        available_count,
        details_status: detail_status,
    }
}

pub(crate) fn classify_http_error(
    status: reqwest::StatusCode,
    payload: Option<&Value>,
    retry_after: Option<&str>,
) -> QuotaFetchError {
    let code = payload.and_then(extract_error_code);
    let code_suffix = code
        .as_deref()
        .map(|code| format!("，错误代码：{code}"))
        .unwrap_or_default();
    let (quota_status, message) = match status.as_u16() {
        400 => (
            QuotaStatus::Error,
            format!(
                "OpenAI 无法处理额度请求（HTTP 400：请求或账号信息无效），请重新登录后再试{code_suffix}"
            ),
        ),
        401 => (
            QuotaStatus::Unauthorized,
            format!(
                "OpenAI 登录凭据无效、已过期或已被撤销（HTTP 401），请刷新登录或重新授权{code_suffix}"
            ),
        ),
        402 if code.as_deref() == Some(DEACTIVATED_WORKSPACE_CODE) => (
            QuotaStatus::Unauthorized,
            format!(
                "账号所属工作区已停用（{DEACTIVATED_WORKSPACE_CODE}，HTTP 402），无法查询额度或启动 Codex，请更换账号或联系工作区管理员。"
            ),
        ),
        402 => (
            QuotaStatus::Unauthorized,
            format!(
                "OpenAI 拒绝提供服务（HTTP 402：工作区、订阅或账单状态不允许访问），请检查账号状态或联系工作区管理员{code_suffix}"
            ),
        ),
        403 => (
            QuotaStatus::Unauthorized,
            format!(
                "OpenAI 拒绝访问（HTTP 403：账号无权限，或工作区/所在地区受限），请更换账号或联系管理员{code_suffix}"
            ),
        ),
        404 => (
            QuotaStatus::Error,
            format!(
                "OpenAI 额度资源不存在（HTTP 404），当前账号可能不支持额度查询，或客户端接口已过期{code_suffix}"
            ),
        ),
        408 => (
            QuotaStatus::Error,
            format!("OpenAI 额度请求超时（HTTP 408），请稍后重试{code_suffix}"),
        ),
        409 => (
            QuotaStatus::Error,
            format!(
                "OpenAI 拒绝额度请求（HTTP 409：账号或工作区状态冲突），请刷新登录后再试{code_suffix}"
            ),
        ),
        422 => (
            QuotaStatus::Error,
            format!(
                "OpenAI 无法处理当前账号的额度信息（HTTP 422），请重新登录或更换账号{code_suffix}"
            ),
        ),
        429 if is_quota_exhausted_code(code.as_deref()) => (
            QuotaStatus::RateLimited,
            format!(
                "OpenAI 额度、余额或工作区支出上限已用尽（HTTP 429），请等待额度重置或检查账单与用量上限{code_suffix}"
            ),
        ),
        429 => {
            let retry = retry_after
                .map(|value| format!("；服务建议在 {value} 后重试"))
                .unwrap_or_default();
            (
                QuotaStatus::RateLimited,
                format!(
                    "OpenAI 请求过于频繁（HTTP 429：触发速率限制），请稍后重试{retry}{code_suffix}"
                ),
            )
        }
        500 => (
            QuotaStatus::Error,
            format!("OpenAI 服务内部错误（HTTP 500），请稍后重试{code_suffix}"),
        ),
        502 => (
            QuotaStatus::Error,
            format!("OpenAI 网关返回无效响应（HTTP 502），请稍后重试{code_suffix}"),
        ),
        503 => (
            QuotaStatus::Error,
            format!("OpenAI 服务暂时过载或不可用（HTTP 503），请稍后重试{code_suffix}"),
        ),
        504 => (
            QuotaStatus::Error,
            format!("OpenAI 网关等待响应超时（HTTP 504），请稍后重试{code_suffix}"),
        ),
        value if (400..500).contains(&value) => (
            QuotaStatus::Error,
            format!("OpenAI 拒绝了额度请求（HTTP {value}：账号、权限或请求状态异常）{code_suffix}"),
        ),
        value if (500..600).contains(&value) => (
            QuotaStatus::Error,
            format!("OpenAI 服务暂时异常（HTTP {value}），请稍后重试{code_suffix}"),
        ),
        value => (
            QuotaStatus::Error,
            format!("OpenAI 额度服务返回异常响应（HTTP {value}）{code_suffix}"),
        ),
    };
    QuotaFetchError {
        status: quota_status,
        message,
        code,
        retryable: false,
    }
}

fn is_quota_exhausted_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some(
            "credit_balance_exhausted"
                | "organization_spend_limit_exceeded"
                | "project_spend_limit_exceeded"
                | "organization_usage_limit_exceeded"
                | "insufficient_quota"
        )
    )
}

fn extract_error_code(payload: &Value) -> Option<String> {
    ["/detail/code", "/detail/error/code", "/error/code", "/code"]
        .into_iter()
        .find_map(|pointer| payload.pointer(pointer).and_then(Value::as_str))
        .map(str::trim)
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 128
                && code
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "_-.".contains(character))
        })
        .map(str::to_owned)
}

pub(crate) async fn read_optional_error_json(response: reqwest::Response) -> Option<Value> {
    read_response_bytes(response)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

fn parse_plan_type(payload: &Value) -> Option<String> {
    payload
        .get("plan_type")
        .or_else(|| payload.get("planType"))
        .or_else(|| payload.get("data").and_then(|data| data.get("plan_type")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn should_send_account_id(account: &StoredOfficialAccount) -> bool {
    let account_id = account.account_id.trim();
    !account_id.is_empty()
        && !(account.source == OfficialAccountSource::ProxyImport
            && account_id.starts_with("proxy-"))
}

pub(crate) async fn read_bounded_json(
    response: reqwest::Response,
) -> Result<Value, QuotaFetchError> {
    let bytes = read_response_bytes(response).await?;
    serde_json::from_slice(&bytes)
        .map_err(|_| QuotaFetchError::new(QuotaStatus::Error, "OpenAI 额度接口没有返回有效 JSON"))
}

pub(crate) async fn read_response_bytes(
    response: reqwest::Response,
) -> Result<Vec<u8>, QuotaFetchError> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|_| QuotaFetchError::new(QuotaStatus::Error, "无法读取 OpenAI 额度响应"))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_QUOTA_RESPONSE_BYTES {
            return Err(QuotaFetchError::new(
                QuotaStatus::Error,
                "OpenAI 额度响应内容过大",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum QuotaParseError {
    Unsupported(String),
    Invalid(String),
}

fn parse_quota_payload(payload: &Value, now: i64) -> Result<QuotaData, QuotaParseError> {
    let rate_limit = payload
        .get("rate_limit")
        .or_else(|| payload.get("data").and_then(|data| data.get("rate_limit")));
    let Some(rate_limit) = rate_limit.filter(|value| !value.is_null()) else {
        return Err(QuotaParseError::Unsupported(
            "OpenAI 返回的数据中没有 5 小时或 7 天用量窗口，该账号暂不支持额度查询。".into(),
        ));
    };
    let primary = rate_limit
        .get("primary_window")
        .filter(|window| !window.is_null())
        .map(|window| parse_window(window, now))
        .transpose()?;
    let secondary = rate_limit
        .get("secondary_window")
        .filter(|window| !window.is_null())
        .map(|window| parse_window(window, now))
        .transpose()?;
    if primary.is_none() && secondary.is_none() {
        return Err(QuotaParseError::Unsupported(
            "OpenAI 返回的 5 小时和 7 天用量窗口均不可用，该账号暂不支持额度查询。".into(),
        ));
    }
    Ok(QuotaData::Windowed { primary, secondary })
}

fn parse_window(value: &Value, now: i64) -> Result<QuotaWindow, QuotaParseError> {
    let used_percent = parse_percent(value.get("used_percent"))?;
    let remaining_percent = (100.0 - used_percent).max(0.0);
    let window_seconds = value
        .get("limit_window_seconds")
        .and_then(parse_number)
        .filter(|seconds| *seconds > 0);
    let reset_at = value.get("reset_at").and_then(parse_timestamp).or_else(|| {
        value
            .get("reset_after_seconds")
            .and_then(parse_number)
            .filter(|seconds| *seconds >= 0)
            .map(|seconds| now.saturating_add(seconds))
    });
    Ok(QuotaWindow {
        used_percent,
        remaining_percent,
        window_seconds,
        reset_at,
    })
}

fn parse_percent(value: Option<&Value>) -> Result<f64, QuotaParseError> {
    let Some(value) = value else {
        return Err(QuotaParseError::Invalid(
            "OpenAI 返回的额度窗口缺少 used_percent 字段".into(),
        ));
    };
    parse_non_negative_f64(value).ok_or_else(|| {
        QuotaParseError::Invalid("OpenAI 返回的额度窗口包含无效的 used_percent".into())
    })
}

fn parse_number(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| parse_non_negative_f64(value).map(|number| number as i64))
}

fn parse_non_negative_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| {
            value
                .as_str()
                .and_then(|text| text.trim().parse::<f64>().ok())
        })
        .filter(|number| number.is_finite() && *number >= 0.0)
}

pub(crate) fn parse_timestamp(value: &Value) -> Option<i64> {
    parse_number(value).or_else(|| {
        value.as_str().and_then(|text| {
            chrono::DateTime::parse_from_rfc3339(text.trim())
                .ok()
                .map(|time| time.timestamp())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CodexAuthCredential, CodexAuthTokens, ProviderAccountQuota};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn serve_usage_response(
        body: &'static str,
        assert_request: impl FnOnce(&str) + Send + 'static,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 8192];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]).to_ascii_lowercase();
            assert_request(&request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        (format!("http://{address}/usage"), server)
    }

    fn serve_error_response(
        status: &'static str,
        headers: &'static str,
        body: &'static str,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 8192];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            )
            .unwrap();
        });
        (format!("http://{address}/usage"), server)
    }

    fn account(source: OfficialAccountSource, account_id: &str) -> StoredOfficialAccount {
        StoredOfficialAccount {
            id: "saved-account".into(),
            name: "Account".into(),
            remark: String::new(),
            account_id: account_id.into(),
            email: String::new(),
            credential: CodexAuthCredential {
                auth_mode: "chatgpt".into(),
                openai_api_key: None,
                tokens: CodexAuthTokens {
                    id_token: String::new(),
                    access_token: "at-test-secret".into(),
                    refresh_token: String::new(),
                    account_id: account_id.into(),
                },
                last_refresh: "2026-07-31T00:00:00Z".into(),
            },
            source,
            expires_at: None,
            quota: ProviderAccountQuota::default(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn fetches_official_windows_with_required_headers() {
        let body = r#"{"rate_limit":{"primary_window":{"used_percent":20,"limit_window_seconds":18000},"secondary_window":{"used_percent":40,"limit_window_seconds":604800}}}"#;
        let (endpoint, server) = serve_usage_response(body, |request| {
            assert!(request.starts_with("get /usage http/1.1"));
            assert!(request.contains("authorization: bearer at-test-secret"));
            assert!(request.contains("chatgpt-account-id: workspace-1"));
            assert!(request.contains("openai-beta: codex-1"));
            assert!(request.contains("originator: codex desktop"));
            assert!(request.contains("user-agent: codex_cli_rs/"));
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let quota = fetch_quota_from(
            &client,
            &account(OfficialAccountSource::OpenAiOauth, "workspace-1"),
            &endpoint,
        )
        .await
        .unwrap();

        let QuotaFetchResult {
            data: quota,
            plan_type,
            ..
        } = quota;
        let QuotaData::Windowed { primary, secondary } = quota;
        assert_eq!(plan_type, None);
        assert_eq!(primary.unwrap().remaining_percent, 80.0);
        assert_eq!(secondary.unwrap().remaining_percent, 60.0);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn marks_unrecognized_payload_as_unsupported() {
        let (endpoint, server) = serve_usage_response(r#"{"usage":{}}"#, |request| {
            assert!(request.starts_with("get /usage http/1.1"));
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let error = fetch_quota_from(
            &client,
            &account(OfficialAccountSource::OpenAiOauth, "workspace-1"),
            &endpoint,
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, QuotaStatus::Unsupported);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn preserves_deactivated_workspace_code_and_explains_http_402() {
        let (endpoint, server) = serve_error_response(
            "402 Payment Required",
            "",
            r#"{"detail":{"code":"deactivated_workspace"}}"#,
        );
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let error = fetch_quota_from(
            &client,
            &account(OfficialAccountSource::OpenAiOauth, "workspace-1"),
            &endpoint,
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, QuotaStatus::Unauthorized);
        assert_eq!(error.code.as_deref(), Some(DEACTIVATED_WORKSPACE_CODE));
        assert!(error.message.contains("工作区已停用"));
        assert!(error.message.contains("HTTP 402"));
        server.join().unwrap();
    }

    #[test]
    fn classifies_common_http_errors_with_actionable_messages() {
        let cases = [
            (401, QuotaStatus::Unauthorized, "登录凭据无效"),
            (403, QuotaStatus::Unauthorized, "账号无权限"),
            (404, QuotaStatus::Error, "额度资源不存在"),
            (408, QuotaStatus::Error, "请求超时"),
            (500, QuotaStatus::Error, "服务内部错误"),
            (502, QuotaStatus::Error, "网关返回无效响应"),
            (503, QuotaStatus::Error, "服务暂时过载或不可用"),
            (504, QuotaStatus::Error, "网关等待响应超时"),
            (418, QuotaStatus::Error, "账号、权限或请求状态异常"),
        ];
        for (status, expected_status, expected_text) in cases {
            let error =
                classify_http_error(reqwest::StatusCode::from_u16(status).unwrap(), None, None);
            assert_eq!(error.status, expected_status, "HTTP {status}");
            assert!(error.message.contains(expected_text), "{}", error.message);
            assert!(error.message.contains(&format!("HTTP {status}")));
        }
    }

    #[test]
    fn distinguishes_rate_limit_from_exhausted_quota() {
        let rate_limit =
            classify_http_error(reqwest::StatusCode::TOO_MANY_REQUESTS, None, Some("30 秒"));
        assert_eq!(rate_limit.status, QuotaStatus::RateLimited);
        assert!(rate_limit.message.contains("触发速率限制"));
        assert!(rate_limit.message.contains("30 秒"));

        let exhausted = classify_http_error(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            Some(&json!({"error": {"code": "project_spend_limit_exceeded"}})),
            None,
        );
        assert_eq!(exhausted.status, QuotaStatus::RateLimited);
        assert_eq!(
            exhausted.code.as_deref(),
            Some("project_spend_limit_exceeded")
        );
        assert!(exhausted.message.contains("支出上限已用尽"));
    }

    #[test]
    fn rejects_deactivated_accounts_before_activation_or_launch() {
        let mut saved = account(OfficialAccountSource::OpenAiOauth, "workspace-1");
        saved.quota.error_code = Some(DEACTIVATED_WORKSPACE_CODE.into());

        let error = ensure_account_usable(&saved).unwrap_err();

        assert!(error.to_string().contains("无法设为当前账号或启动 Codex"));
    }

    #[test]
    fn accepts_percent_variants_and_missing_window_fields() {
        let window = parse_window(
            &json!({
                "used_percent": "125.5",
                "limit_window_seconds": "18000"
            }),
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(window.used_percent, 125.5);
        assert_eq!(window.remaining_percent, 0.0);
        assert_eq!(window.window_seconds, Some(18_000));
        assert_eq!(window.reset_at, None);

        let window = parse_window(&json!({"used_percent": 20}), 1_700_000_000).unwrap();
        assert_eq!(window.used_percent, 20.0);
        assert_eq!(window.remaining_percent, 80.0);
        assert_eq!(window.window_seconds, None);
        assert_eq!(window.reset_at, None);
    }

    #[test]
    fn parses_positive_numbers_and_rejects_invalid_percent_values() {
        assert_eq!(parse_percent(Some(&json!("25.5"))).unwrap(), 25.5);
        assert_eq!(parse_percent(Some(&json!(0.0))).unwrap(), 0.0);
        assert_eq!(parse_number(&json!("12.75")), Some(12));

        assert!(matches!(
            parse_percent(None),
            Err(QuotaParseError::Invalid(_))
        ));
        assert!(matches!(
            parse_percent(Some(&json!(-1.0))),
            Err(QuotaParseError::Invalid(_))
        ));
        assert!(matches!(
            parse_percent(Some(&json!("NaN"))),
            Err(QuotaParseError::Invalid(_))
        ));
        assert!(matches!(
            parse_percent(Some(&json!("inf"))),
            Err(QuotaParseError::Invalid(_))
        ));
    }

    #[test]
    fn computes_reset_at_from_numeric_and_rfc3339_fields() {
        let window = parse_window(
            &json!({
                "used_percent": 10,
                "reset_after_seconds": 3600
            }),
            1000,
        )
        .unwrap();
        assert_eq!(window.reset_at, Some(4600));

        let window = parse_window(
            &json!({
                "used_percent": 10,
                "reset_at": "2026-08-04T12:00:00Z"
            }),
            0,
        )
        .unwrap();
        assert_eq!(
            window.reset_at,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-08-04T12:00:00Z")
                    .unwrap()
                    .timestamp()
            )
        );
    }

    #[test]
    fn classifies_unrecognized_and_invalid_payloads() {
        let error = parse_quota_payload(&json!({"data": {"usage": []}}), 0).unwrap_err();
        assert!(matches!(error, QuotaParseError::Unsupported(_)));

        let error = parse_quota_payload(
            &json!({"rate_limit": {"primary_window": null, "secondary_window": null}}),
            0,
        )
        .unwrap_err();
        assert!(matches!(error, QuotaParseError::Unsupported(_)));

        let error = parse_quota_payload(
            &json!({"data": {"rate_limit": {"primary_window": {"used_percent": -1}}}}),
            0,
        )
        .unwrap_err();
        assert!(matches!(error, QuotaParseError::Invalid(_)));
    }

    #[test]
    fn accepts_rate_limit_under_data_wrapper() {
        let quota = parse_quota_payload(
            &json!({
                "data": {
                    "rate_limit": {
                        "primary_window": {"used_percent": 30, "limit_window_seconds": 18000}
                    }
                }
            }),
            0,
        )
        .unwrap();
        let QuotaData::Windowed { primary, secondary } = quota;
        assert_eq!(primary.unwrap().remaining_percent, 70.0);
        assert!(secondary.is_none());
    }

    #[test]
    fn omits_synthetic_proxy_account_id() {
        assert!(!should_send_account_id(&account(
            OfficialAccountSource::ProxyImport,
            "proxy-0123456789"
        )));
        assert!(should_send_account_id(&account(
            OfficialAccountSource::ProxyImport,
            "workspace-1"
        )));
    }

    #[test]
    fn marks_connection_failures_as_safe_to_retry() {
        assert!(QuotaFetchError::retryable("暂时不可达").is_retryable());
        assert!(!QuotaFetchError::new(QuotaStatus::Error, "响应无效").is_retryable());
    }
}
