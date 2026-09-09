use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub(crate) const MAX_DISPLAY_NAME_CHARS: usize = 100;
pub(crate) const MAX_ACCOUNT_REMARK_CHARS: usize = 200;
pub(crate) const MAX_ACCOUNT_ID_CHARS: usize = 512;
pub(crate) const MAX_CREDENTIAL_CHARS: usize = 262_144;
const MAX_API_URL_CHARS: usize = 2_048;
const MAX_API_KEY_CHARS: usize = 65_536;
const MAX_MODEL_ID_CHARS: usize = 512;
const MAX_CONTEXT_WINDOW_OVERRIDE: u64 = 9_007_199_254_740_991;
const MAX_CUSTOM_HEADERS: usize = 64;
const MAX_HEADER_NAME_CHARS: usize = 256;
const MAX_HEADER_VALUE_CHARS: usize = 8_192;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    InvalidConfig(String),
    #[error("Codex 配置在本次操作期间发生了变化，请重新检查后再试。")]
    StaleOperation,
    #[error("{0}")]
    Internal(String),
}

impl Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TokenIdentity {
    pub subject: Option<String>,
    pub account_id: Option<String>,
    pub email: Option<String>,
    pub expires_at: Option<i64>,
}

pub(crate) fn token_local_identity(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut result = String::with_capacity(30);
    result.push_str("proxy-");
    for &byte in &digest[..12] {
        result.push(char::from_digit(u32::from(byte >> 4), 16).expect("半字节必为十六进制"));
        result.push(char::from_digit(u32::from(byte & 0xF), 16).expect("半字节必为十六进制"));
    }
    result
}

/// Stable, non-reversible identity used by usage aggregation.  The external
/// OpenAI account id is never exposed as the aggregation key returned to the UI.
pub(crate) fn credential_subject(credential: &CodexAuthCredential) -> Option<String> {
    [
        credential.tokens.id_token.as_str(),
        credential.tokens.access_token.as_str(),
    ]
    .into_iter()
    .filter_map(token_identity)
    .find_map(|identity| identity.subject)
}

fn credential_email(credential: &CodexAuthCredential) -> Option<String> {
    [
        credential.tokens.id_token.as_str(),
        credential.tokens.access_token.as_str(),
    ]
    .into_iter()
    .filter_map(token_identity)
    .find_map(|identity| identity.email)
}

/// Checks a credential update without allowing a workspace id shared by two
/// users to stand in for user identity. Legacy credentials that both lack a
/// subject retain their previous workspace-only behavior.
pub(crate) fn official_credential_identity_matches(
    account: &StoredOfficialAccount,
    credential: &CodexAuthCredential,
) -> bool {
    if account.account_id.trim() != credential.tokens.account_id.trim() {
        return false;
    }
    match (
        credential_subject(&account.credential),
        credential_subject(credential),
    ) {
        (Some(saved), Some(incoming)) => saved == incoming,
        (None, None) => true,
        _ => {
            let saved_email = account.email.trim();
            !saved_email.is_empty()
                && credential_email(credential)
                    .is_some_and(|email| saved_email.eq_ignore_ascii_case(email.trim()))
        }
    }
}

/// Stable account scope used for account-specific aggregation.
/// A ChatGPT account id identifies a workspace, so users in a shared workspace
/// must also be separated by their token subject (or legacy email fallback).
pub(crate) fn official_account_identity_scope(account: &StoredOfficialAccount) -> String {
    let workspace = account.account_id.trim();
    if let Some(subject) = credential_subject(&account.credential) {
        return format!("{workspace}\0sub:{}", subject.trim());
    }
    let email = account.email.trim();
    if !email.is_empty() {
        return format!("{workspace}\0email:{}", email.to_lowercase());
    }
    workspace.to_owned()
}

pub(crate) fn canonical_official_account_id(account: &StoredOfficialAccount) -> String {
    let digest = Sha256::digest(official_account_identity_scope(account).as_bytes());
    let short = digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("official-{short}")
}

pub(crate) fn token_identity(token: &str) -> Option<TokenIdentity> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let auth = claims.get("https://api.openai.com/auth");
    let profile = claims.get("https://api.openai.com/profile");
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let account_id = claims
        .get("chatgpt_account_id")
        .and_then(Value::as_str)
        .or_else(|| {
            auth.and_then(|value| value.get("chatgpt_account_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            claims
                .pointer("/organizations/0/id")
                .and_then(Value::as_str)
        })
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let email = claims
        .get("email")
        .and_then(Value::as_str)
        .or_else(|| {
            profile
                .and_then(|value| value.get("email"))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let expires_at = claims.get("exp").and_then(Value::as_i64);
    Some(TokenIdentity {
        subject,
        account_id,
        email,
        expires_at,
    })
}

/// auth.json 未保存 OAuth 的 expires_in；同步时仅从 JWT 的 exp 推断，
/// 并优先使用 access token，再使用 id token。
pub(crate) fn credential_expires_at(credential: &CodexAuthCredential) -> Option<i64> {
    token_identity(&credential.tokens.access_token)
        .and_then(|identity| identity.expires_at)
        .or_else(|| {
            token_identity(&credential.tokens.id_token).and_then(|identity| identity.expires_at)
        })
}

fn official_account_subject(account: &StoredOfficialAccount) -> Option<String> {
    credential_subject(&account.credential)
}

/// Accounts are scoped to a ChatGPT workspace, then distinguished by the
/// OpenAI user. Older credentials without a `sub` claim fall back to email;
/// workspace-only matching is safe only when neither side has a user identity.
pub(crate) fn official_account_identity_matches(
    left: &StoredOfficialAccount,
    right: &StoredOfficialAccount,
) -> bool {
    if left.account_id.trim() != right.account_id.trim() {
        return false;
    }

    let left_subject = official_account_subject(left);
    let right_subject = official_account_subject(right);
    if let (Some(left_subject), Some(right_subject)) = (&left_subject, &right_subject) {
        return left_subject == right_subject;
    }

    let left_email = left.email.trim();
    let right_email = right.email.trim();
    if !left_email.is_empty() && !right_email.is_empty() {
        return left_email.eq_ignore_ascii_case(right_email);
    }

    left_subject.is_none()
        && right_subject.is_none()
        && left_email.is_empty()
        && right_email.is_empty()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProviderApiType {
    /// 服务直接提供 OpenAI Responses API，Codex 直连。
    #[default]
    Responses,
    /// 服务只提供 Chat Completions API，通过本机转换代理接入。
    Chat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfile {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub active: bool,
    /// 旧版手工默认模型字段，仅为兼容已有 JSON/IPC 数据保留。
    /// 保存时会清空，运行时模型只从 `available_models` 中选择。
    #[serde(default)]
    pub model: String,
    /// 从服务 `/models` 接口读取到的模型上下文窗口（token）；
    /// 写入模型目录时优先使用，没有返回时回退 Codex 默认值。
    #[serde(default)]
    pub model_context_windows: BTreeMap<String, u64>,
    /// 用户为当前服务手动指定的统一上下文窗口（token）。为空时继续使用
    /// `/models`、models.dev 和默认值组成的自动匹配链。
    #[serde(default)]
    pub context_window_override: Option<u64>,
    /// 服务 `/models` 接口返回的可用模型列表（保存服务时静默获取，用户无感知）；
    /// 只保存接口实际返回的模型 id，是模型目录的唯一来源。
    #[serde(default)]
    pub available_models: Vec<String>,
    /// 写入 Codex 的模型列表；为空时默认使用全部 available_models。
    #[serde(default)]
    pub selected_models: Option<Vec<String>>,
    /// 用户手动添加的自定义模型 id（独立于 /models 同步的 available_models）；
    /// 与 available_models 一起构成该服务可写入 Codex 的“有效模型”。
    #[serde(default)]
    pub custom_models: Vec<String>,
    /// 从 models.dev（catalog.json）抓取的本服务商模型元数据（slug → 元数据）；
    /// 只在 id 与服务 `/models` 接口返回完全一致时保留，用于补充窗口/简介/名称。
    #[serde(default)]
    pub models_dev_meta: BTreeMap<String, ProviderModelsDevMeta>,
    /// 服务接入方式：直接 Responses API，或经本机转换代理接入 Chat Completions API。
    #[serde(default)]
    pub api_type: ProviderApiType,
    /// 该服务对应的 API Key；一个服务对应一个 Key。
    #[serde(default)]
    pub api_key: Option<String>,
    /// 是否已保存 API Key；脱敏返回时只暴露此布尔值，不泄露密钥。
    #[serde(default)]
    pub has_api_key: bool,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

/// WebView 保存第三方服务时允许提交的字段。模型目录、模型元数据、
/// active/hasApiKey 和时间戳全部由后端维护；模型选择可由用户编辑。
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSaveInput {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_type: ProviderApiType,
    #[serde(default)]
    pub api_key: Option<String>,
    /// `null`（或兼容旧调用方时缺省）明确表示使用全部有效模型；数组表示指定子集。
    #[serde(default)]
    pub selected_models: Option<Vec<String>>,
    /// 用户手动添加的自定义模型 id；随保存提交，刷新 /models 时不会被清空。
    #[serde(default)]
    pub custom_models: Option<Vec<String>>,
    /// 用户为当前服务显式指定的统一上下文窗口；`null`/缺省表示自动匹配。
    #[serde(default)]
    pub context_window_override: Option<u64>,
}

impl From<ProviderSaveInput> for ProviderProfile {
    fn from(input: ProviderSaveInput) -> Self {
        Self {
            id: input.id,
            name: input.name,
            base_url: input.base_url,
            headers: input.headers,
            timeout_secs: input.timeout_secs,
            enabled: input.enabled,
            active: false,
            model: String::new(),
            model_context_windows: BTreeMap::new(),
            context_window_override: input.context_window_override,
            available_models: Vec::new(),
            selected_models: input.selected_models,
            custom_models: input.custom_models.unwrap_or_default(),
            models_dev_meta: BTreeMap::new(),
            api_type: input.api_type,
            api_key: input.api_key,
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl ProviderProfile {
    pub fn normalize_and_validate(&mut self) -> Result<(), AppError> {
        self.name = self.name.trim().to_owned();
        self.base_url = self.base_url.trim().trim_end_matches('/').to_owned();
        if self.name.is_empty() || self.base_url.is_empty() {
            return Err(AppError::InvalidConfig(
                "请填写服务名称和 API 地址。".into(),
            ));
        }
        ensure_char_limit(
            &self.name,
            MAX_DISPLAY_NAME_CHARS,
            "服务名称不能超过 100 个字符。",
        )?;
        ensure_char_limit(
            &self.base_url,
            MAX_API_URL_CHARS,
            "API 地址不能超过 2,048 个字符。",
        )?;
        let url = reqwest::Url::parse(&self.base_url).map_err(|_| {
            AppError::InvalidConfig(
                "API 地址格式不正确，请填写完整的 http:// 或 https:// 地址。".into(),
            )
        })?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(AppError::InvalidConfig(
                "API 地址必须以 http:// 或 https:// 开头。".into(),
            ));
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(AppError::InvalidConfig(
                "API 地址中不能包含用户名、密码、查询参数或 # 片段。".into(),
            ));
        }
        let host = url.host_str().unwrap_or_default();
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if url.scheme() != "https" && !loopback {
            return Err(AppError::InvalidConfig(
                "远程 API 必须使用 HTTPS；只有本机地址可以使用 HTTP。".into(),
            ));
        }
        self.timeout_secs = self.timeout_secs.clamp(1, 600);
        if self
            .context_window_override
            .is_some_and(|window| window == 0 || window > MAX_CONTEXT_WINDOW_OVERRIDE)
        {
            return Err(AppError::InvalidConfig(
                "上下文窗口必须是 1 到 9,007,199,254,740,991 之间的整数 token。".into(),
            ));
        }
        validate_headers(&self.headers)?;
        self.custom_models = Self::normalize_custom_models(
            std::mem::take(&mut self.custom_models),
            &self.available_models,
            MAX_MODEL_ID_CHARS,
        )?;
        if let Some(key) = self.api_key.as_deref() {
            let key = key.trim();
            if key.is_empty() {
                self.api_key = None;
            } else {
                ensure_char_limit(key, MAX_API_KEY_CHARS, "API Key 不能超过 65,536 个字符。")?;
                self.api_key = Some(key.to_owned());
            }
        }
        Ok(())
    }

    fn normalize_custom_models(
        raw: Vec<String>,
        available: &[String],
        max_chars: usize,
    ) -> Result<Vec<String>, AppError> {
        let available_set: std::collections::HashSet<&str> =
            available.iter().map(String::as_str).collect();
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for value in raw {
            let value = value.trim();
            if value.is_empty() || available_set.contains(value) || !seen.insert(value.to_owned()) {
                continue;
            }
            ensure_char_limit(value, max_chars, "自定义模型 ID 不能超过 512 个字符。")?;
            result.push(value.to_owned());
        }
        Ok(result)
    }

    pub fn redacted(mut self) -> Self {
        self.has_api_key = self.api_key.is_some();
        self.api_key = None;
        redact_header_values(&mut self.headers);
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OfficialAccountSource {
    #[default]
    OpenAiOauth,
    ProxyImport,
}

/// 凭据维护仅对前端公开状态和时间，不会序列化任何 Token 或错误响应内容。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CredentialRefreshStatus {
    #[default]
    Unknown,
    Healthy,
    ManagedByCodex,
    WaitingRetry,
    ReauthenticationRequired,
    NotRefreshable,
}

/// 在线登录检查与本地凭据维护分开记录。旧的 `healthy` 维护状态不会被当作有效登录。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LoginVerificationStatus {
    #[default]
    Unknown,
    Valid,
    Invalid,
    WorkspaceOrPermission,
    CheckFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRefreshState {
    #[serde(default)]
    pub status: CredentialRefreshStatus,
    #[serde(default)]
    pub last_attempt_at: Option<i64>,
    #[serde(default)]
    pub last_success_at: Option<i64>,
    #[serde(default)]
    pub next_retry_at: Option<i64>,
    #[serde(default)]
    pub retry_count: u8,
    #[serde(default)]
    pub last_refresh_at: Option<i64>,
    #[serde(default)]
    pub last_sync_at: Option<i64>,
    #[serde(default)]
    pub last_check_at: Option<i64>,
    #[serde(default)]
    pub verification: LoginVerificationStatus,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialMaintenanceOutcome {
    Refreshed,
    SyncedFromCodex,
    ManagedByCodex,
    Unchanged,
    WaitingRetry,
    ReauthenticationRequired,
    NotRefreshable,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialMaintenanceResult {
    pub account: OfficialAccountView,
    pub outcome: CredentialMaintenanceOutcome,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QuotaStatus {
    #[default]
    Never,
    Success,
    Unsupported,
    Unauthorized,
    RateLimited,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccountQuota {
    #[serde(default)]
    pub status: QuotaStatus,
    pub data: Option<QuotaData>,
    pub plan_type: Option<String>,
    pub fetched_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    pub error: Option<String>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub estimates: Vec<QuotaEstimate>,
    /// OpenAI 额度接口返回的重置卡摘要。`None` 表示旧数据或服务没有提供，不能当作 0 张。
    #[serde(default)]
    pub reset_credits: ResetCreditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResetCreditDetailsStatus {
    #[default]
    Unknown,
    Complete,
    Partial,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResetCreditSummary {
    /// 服务端摘要的权威数量；缺失时必须保留未知状态。
    pub available_count: Option<u32>,
    #[serde(default)]
    pub details_status: ResetCreditDetailsStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetCredit {
    pub id: String,
    pub reset_type: Option<String>,
    pub status: Option<String>,
    pub granted_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetCreditDetails {
    pub account_id: String,
    pub summary: ResetCreditSummary,
    pub credits: Vec<ResetCredit>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResetCreditConsumeOutcome {
    Reset,
    AlreadyRedeemed,
    NothingToReset,
    NoCredit,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetCreditConsumeResult {
    pub outcome: ResetCreditConsumeOutcome,
    pub details: ResetCreditDetails,
    pub quota: Option<ProviderAccountQuota>,
    pub refresh_error: Option<String>,
}

/// 本机依据已记录用量推算出的额度金额。它不是 OpenAI 返回的账单数据，
/// 因此必须同时保存对应额度窗口的身份，避免跨窗口复用旧结论。
pub const CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEstimate {
    pub window_seconds: i64,
    pub reset_at: i64,
    pub estimated_total_microusd: u64,
    pub estimated_at: i64,
    /// 缺少该字段的历史持久化结果按 0 处理，加载时会被自动撤销。
    #[serde(default)]
    pub calculation_version: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEstimateWindowResult {
    pub window_seconds: i64,
    pub reset_at: i64,
    pub success: bool,
    pub estimate: Option<QuotaEstimate>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEstimateResult {
    pub windows: Vec<QuotaEstimateWindowResult>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaRefreshResult {
    pub account_id: String,
    pub quota: ProviderAccountQuota,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ActiveKind {
    #[default]
    None,
    Provider,
    Official,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActiveState {
    pub kind: ActiveKind,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
}

/// Token payload written to Codex's `auth.json` when an official account is
/// activated. This type is deliberately never exposed by a Tauri command.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CodexAuthTokens {
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: String,
    pub account_id: String,
}

impl fmt::Debug for CodexAuthTokens {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexAuthTokens")
            .field("id_token", &"[REDACTED]")
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("account_id", &self.account_id)
            .finish()
    }
}

/// Exact credential object expected by Codex in `auth.json`.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CodexAuthCredential {
    pub auth_mode: String,
    #[serde(default, rename = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String>,
    pub tokens: CodexAuthTokens,
    pub last_refresh: String,
}

impl fmt::Debug for CodexAuthCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexAuthCredential")
            .field("auth_mode", &self.auth_mode)
            .field("OPENAI_API_KEY", &"[REDACTED]")
            .field("tokens", &"[REDACTED]")
            .field("last_refresh", &self.last_refresh)
            .finish()
    }
}

/// Sensitive official-account record. It is serialized only as part of
/// `app.yaml`; public commands must return [`OfficialAccountView`] instead.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StoredOfficialAccount {
    #[serde(default)]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub remark: String,
    pub account_id: String,
    pub email: String,
    pub credential: CodexAuthCredential,
    #[serde(default)]
    pub source: OfficialAccountSource,
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub quota: ProviderAccountQuota,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

impl fmt::Debug for StoredOfficialAccount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredOfficialAccount")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("remark", &self.remark)
            .field("account_id", &self.account_id)
            .field("email", &self.email)
            .field("credential", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccountRemarkUpdate {
    pub id: String,
    pub remark: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OfficialAccountView {
    pub id: String,
    pub name: String,
    pub remark: String,
    pub account_id: String,
    pub email: String,
    pub source: OfficialAccountSource,
    pub expires_at: Option<i64>,
    pub credential_refresh: CredentialRefreshState,
    pub quota: ProviderAccountQuota,
    pub created_at: i64,
    pub updated_at: i64,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOverview {
    pub providers: Vec<ProviderProfile>,
    pub official_accounts: Vec<OfficialAccountView>,
}

impl StoredOfficialAccount {
    pub fn display_name(&self) -> &str {
        let remark = self.remark.trim();
        if remark.is_empty() {
            &self.name
        } else {
            remark
        }
    }

    pub fn view_with_credential_refresh(
        &self,
        active: bool,
        credential_refresh: CredentialRefreshState,
    ) -> OfficialAccountView {
        OfficialAccountView {
            id: self.id.clone(),
            name: self.name.clone(),
            remark: self.remark.clone(),
            account_id: self.account_id.clone(),
            email: self.email.clone(),
            source: self.source,
            expires_at: self.expires_at,
            credential_refresh,
            quota: self.quota.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            active,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OpenAiDeviceAuthorization {
    pub operation_id: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_at: i64,
    pub interval_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OpenAiDevicePoll {
    Pending,
    Expired,
    Complete { account: Box<OfficialAccountView> },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexPreferences {
    #[serde(default)]
    pub home: String,
    /// 手动指定的 Codex 桌面应用路径（.app 目录或可执行文件）；
    /// 为空时自动检测安装位置。
    #[serde(default)]
    pub app_path: Option<String>,
    /// 上次以调试模式启动 Codex 时使用的随机 CDP 端口。端口每次启动随机
    /// 生成（不固定、不可预测），这里持久化以便应用重启后仍能定位
    /// 正在运行的调试实例。
    #[serde(default)]
    pub last_debug_port: Option<u16>,
    /// 最近一次由本应用写入 config.toml 的服务模型。切换到 OpenAI 时只清除
    /// 与这条记录一致的模型，避免误删用户手动设置的值。
    #[serde(default)]
    pub last_managed_model: Option<String>,
}

/// Codex 应用路径设置视图：手动配置 + 实际检测结果。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexAppSetting {
    pub configured: Option<String>,
    pub detected: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    #[serde(default)]
    pub codex: CodexPreferences,
    #[serde(default)]
    pub active: ActiveState,
    #[serde(default)]
    pub providers: Vec<ProviderProfile>,
    #[serde(default)]
    pub official_accounts: Vec<StoredOfficialAccount>,
    /// 独立保存维护元数据，避免把脱敏状态写入任一敏感凭据对象。
    #[serde(default)]
    pub credential_refresh: BTreeMap<String, CredentialRefreshState>,
    /// 已删除连接的非敏感稳定标识；由独立 tombstone 文件持久化，避免旧版
    /// connections.json 覆盖后把用户主动删除的连接重新导入。
    #[serde(default)]
    pub(crate) deletion_tombstones: DeletionTombstones,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeletionTombstones {
    #[serde(default)]
    pub(crate) provider_ids: BTreeSet<String>,
    #[serde(default)]
    pub(crate) official_account_ids: BTreeSet<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTestResult {
    pub ok: bool,
    pub status: u16,
    pub endpoint: String,
    pub message: String,
    pub suggest_v1: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QuotaWindow {
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub window_seconds: Option<i64>,
    pub reset_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum QuotaData {
    Windowed {
        primary: Option<QuotaWindow>,
        secondary: Option<QuotaWindow>,
    },
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub provider_count: usize,
    pub active_provider: Option<String>,
    pub active_kind: ActiveKind,
    pub active_account_id: Option<String>,
    pub active_account: Option<String>,
    pub active_model: Option<String>,
    pub active_quota: Option<ProviderAccountQuota>,
    pub codex_home: String,
    pub database_count: usize,
    pub session_count: usize,
    pub database_health: String,
    pub today_usage: TokenBreakdown,
    pub today_requests: u64,
    pub today_estimated_cost_microusd: u64,
    pub today_subscription_tokens: u64,
    pub today_unpriced_tokens: u64,
    pub today_partial_tokens: u64,
    pub today_unattributed_tokens: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageSourceKind {
    Official,
    Provider,
    Unattributed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageGroupBy {
    Model,
    Account,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostStatus {
    Estimated,
    Subscription,
    Unpriced,
    Partial,
    Unattributed,
    Zero,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PricingScopeKind {
    AccountModel,
    ProviderModel,
    GlobalModel,
    ProviderDefault,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PricingMatchKind {
    Exact,
    Prefix,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BillingMode {
    Token,
    Subscription,
    Unpriced,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageRange {
    pub start_at_ms: i64,
    pub end_at_ms: i64,
}

impl UsageRange {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.start_at_ms < 0 || self.end_at_ms <= self.start_at_ms {
            return Err(AppError::InvalidConfig("用量查询时间范围无效。".into()));
        }
        if self
            .end_at_ms
            .checked_sub(self.start_at_ms)
            .is_none_or(|duration| duration > 366 * 24 * 60 * 60 * 1_000)
        {
            return Err(AppError::InvalidConfig(
                "用量查询范围不能超过 366 天。".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageQuery {
    pub range: UsageRange,
    pub group_by: UsageGroupBy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenBreakdown {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTotals {
    pub tokens: TokenBreakdown,
    pub requests: u64,
    pub estimated_cost_microusd: u64,
    pub subscription_tokens: u64,
    pub unpriced_tokens: u64,
    pub partial_tokens: u64,
    pub unattributed_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRow {
    pub key: String,
    pub model: String,
    pub source_kind: UsageSourceKind,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub source_name: String,
    pub tokens: TokenBreakdown,
    pub requests: u64,
    pub estimated_cost_microusd: Option<u64>,
    pub cost_status: CostStatus,
    pub pricing_rule_name: Option<String>,
    pub pricing_rule_version: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageWarning {
    pub path: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageOverview {
    pub range: UsageRange,
    pub totals: UsageTotals,
    pub rows: Vec<UsageRow>,
    pub models: Vec<String>,
    pub last_refreshed_at_ms: Option<i64>,
    pub collection_started_at_ms: Option<i64>,
    pub collection_started_version: Option<String>,
    pub warnings: Vec<UsageWarning>,
    /// 按本机自然日或小时聚合的趋势点；与 totals 同一次查询产出，
    /// 避免查询后再对同一范围做第二趟全量扫描。
    pub trend_points: Vec<UsageTrendPoint>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTrend {
    pub range: UsageRange,
    pub points: Vec<UsageTrendPoint>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTrendPoint {
    pub day_start_ms: i64,
    pub tokens: TokenBreakdown,
    pub requests: u64,
    pub estimated_cost_microusd: u64,
    pub unpriced_tokens: u64,
    pub partial_tokens: u64,
    pub unattributed_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OfficialPricingCatalogView {
    pub status: String,
    pub source_url: String,
    pub version: Option<i64>,
    pub content_sha256: Option<String>,
    pub fetched_at_ms: Option<i64>,
    pub etag: Option<String>,
    pub model_count: usize,
    pub models: Vec<String>,
    pub rates: Vec<OfficialModelRateView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OfficialModelRateView {
    pub model: String,
    pub long_context_threshold: Option<u64>,
    pub short: TokenRatesView,
    pub long: Option<TokenRatesView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRatesView {
    pub input: Option<i64>,
    pub cached_input: Option<i64>,
    pub cache_write: Option<i64>,
    pub output: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRefreshResult {
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub files_opened: usize,
    pub events_added: usize,
    pub events_skipped: usize,
    pub events_pruned: usize,
    pub partial_lines: usize,
    pub warnings: Vec<UsageWarning>,
    pub last_refreshed_at_ms: i64,
    pub elapsed_ms: u64,
    pub retention_days: i64,
    pub database_compacted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PricingRule {
    pub id: String,
    pub version: u64,
    pub active: bool,
    pub scope_kind: PricingScopeKind,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub model_pattern: String,
    pub match_kind: PricingMatchKind,
    pub billing_mode: BillingMode,
    pub input_usd_per_million: Option<String>,
    pub cached_read_usd_per_million: Option<String>,
    pub cache_write_usd_per_million: Option<String>,
    pub output_usd_per_million: Option<String>,
    pub request_fee_usd: Option<String>,
    pub cache_write_included_in_input: bool,
    pub effective_from_ms: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

pub type SavePricingRule = PricingRule;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PricingScope {
    pub scope_kind: PricingScopeKind,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationRecord {
    pub effective_at_ms: i64,
    pub source_kind: UsageSourceKind,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub model_provider: Option<String>,
    pub display_name_snapshot: String,
    pub auth_source: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepriceResult {
    pub events_repriced: usize,
    pub estimated_cost_microusd: u64,
    pub unpriced_events: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    pub identity: String,
    pub id: String,
    pub title: String,
    pub provider: String,
    pub cwd: String,
    pub archived: bool,
    pub updated_at: i64,
    pub source_db: String,
    pub source_rollout: Option<String>,
    pub original_provider: String,
    pub has_user_event: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageResult<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub page: usize,
    pub page_size: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairTarget {
    pub id: String,
    pub sources: Vec<String>,
    pub current: bool,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseScan {
    pub path: String,
    pub schema: String,
    pub thread_count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairScan {
    pub current_provider: String,
    pub targets: Vec<RepairTarget>,
    pub rollout_files: usize,
    pub session_meta_count: usize,
    pub databases: Vec<DatabaseScan>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RepairResult {
    pub target_provider: String,
    pub files_scanned: usize,
    pub files_cached: usize,
    pub files_opened: usize,
    pub files_modified: usize,
    pub files_skipped: usize,
    pub files_failed: usize,
    pub session_meta_updated: usize,
    pub rows_updated: usize,
    pub databases_scanned: usize,
    pub databases_updated: usize,
    pub warnings: Vec<String>,
    pub repair_complete: bool,
    pub verification_passed: bool,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigPatchPreview {
    pub operation_id: String,
    pub target_path: String,
    pub base_hash: String,
    pub rendered: String,
    pub changes: Vec<String>,
    pub api_key_masked: String,
}

/// 注入到 Codex 渲染进程 / 写入 `model-catalogs` 的单个模型条目；
/// 只作为内部数据，不直接暴露给前端（前端只展示 slug 列表）。
/// 字段与 Codex CLI 的 `model_catalog_json` 格式兼容（snake_case）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CodexModelInfo {
    pub slug: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_reasoning_level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_reasoning_levels: Option<Vec<ReasoningLevelInfo>>,
    /// CLI catalog 必需字段：系统提示词。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    /// CLI catalog 必需字段：`list` 表示出现在选择器中。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_in_api: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_type: Option<String>,
    // 以下为 CLI `model_catalog_json` 必需的其余字段，带默认值保证总是序列化。
    #[serde(default = "default_support_verbosity")]
    pub support_verbosity: bool,
    #[serde(default = "default_default_verbosity")]
    pub default_verbosity: String,
    #[serde(default = "default_apply_patch_tool_type")]
    pub apply_patch_tool_type: String,
    #[serde(default = "default_web_search_tool_type")]
    pub web_search_tool_type: String,
    #[serde(default = "default_input_modalities")]
    pub input_modalities: Vec<String>,
    #[serde(default = "default_supports_image_detail_original")]
    pub supports_image_detail_original: bool,
    #[serde(default = "default_truncation_policy")]
    pub truncation_policy: Value,
    #[serde(default = "default_supports_parallel_tool_calls")]
    pub supports_parallel_tool_calls: bool,
    #[serde(default)]
    pub experimental_supported_tools: Vec<String>,
}

fn default_support_verbosity() -> bool {
    true
}
fn default_default_verbosity() -> String {
    "low".into()
}
fn default_apply_patch_tool_type() -> String {
    "freeform".into()
}
fn default_web_search_tool_type() -> String {
    "text_and_image".into()
}
fn default_input_modalities() -> Vec<String> {
    vec!["text".into(), "image".into()]
}
fn default_supports_image_detail_original() -> bool {
    true
}
fn default_truncation_policy() -> Value {
    serde_json::json!({ "mode": "tokens", "limit": 10000 })
}
fn default_supports_parallel_tool_calls() -> bool {
    true
}

impl Default for CodexModelInfo {
    fn default() -> Self {
        Self {
            slug: String::new(),
            display_name: String::new(),
            description: None,
            context_window: None,
            max_context_window: None,
            default_reasoning_level: None,
            supported_reasoning_levels: None,
            base_instructions: None,
            visibility: None,
            supported_in_api: None,
            priority: None,
            shell_type: None,
            support_verbosity: default_support_verbosity(),
            default_verbosity: default_default_verbosity(),
            apply_patch_tool_type: default_apply_patch_tool_type(),
            web_search_tool_type: default_web_search_tool_type(),
            input_modalities: default_input_modalities(),
            supports_image_detail_original: default_supports_image_detail_original(),
            truncation_policy: default_truncation_policy(),
            supports_parallel_tool_calls: default_supports_parallel_tool_calls(),
            experimental_supported_tools: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReasoningLevelInfo {
    pub effort: String,
    pub description: Option<String>,
}

/// models.dev（catalog.json）中单个模型的元数据，只保存与 `/models` 接口
/// 完全一致 id 的条目。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelsDevMeta {
    pub name: Option<String>,
    pub context_window: Option<u64>,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUnlockStatus {
    /// 是否找到 Codex 桌面应用安装。
    pub app_found: bool,
    /// Codex 桌面应用当前是否在运行。
    pub app_running: bool,
    /// 检测到的 CDP 调试端口；None 表示没有可注入的实例。
    pub debug_port: Option<u16>,
    /// 解锁脚本是否已注入并生效。
    pub injected: bool,
    /// 解锁目录中的模型数量。
    pub model_count: usize,
    /// 解锁目录中的模型 slug 列表（去重、排序）。
    pub models: Vec<String>,
    /// 需要用户注意的提示；无异常时为 None。
    pub warning: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUnlockResult {
    pub port: u16,
    pub injected: bool,
    pub model_count: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigInspection {
    pub path: String,
    pub valid: bool,
    pub active_provider: Option<String>,
    pub managed_provider_present: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsOverview {
    pub inspection: ConfigInspection,
    pub diagnostics: serde_json::Value,
    pub can_preview_custom: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportDiagnostics {
    pub schema_version: u32,
    pub generated_at: String,
    pub app: SupportAppDiagnostics,
    pub system: SupportSystemDiagnostics,
    pub paths: SupportPathDiagnostics,
    pub configuration: SupportConfigDiagnostics,
    pub connection: SupportConnectionDiagnostics,
    pub storage: SupportStorageDiagnostics,
    pub network: SupportNetworkDiagnostics,
    pub warnings: Vec<String>,
    pub privacy: SupportPrivacyDiagnostics,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportAppDiagnostics {
    pub name: String,
    pub version: String,
    pub build_profile: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportSystemDiagnostics {
    pub os: String,
    pub architecture: String,
    pub family: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportPathDiagnostics {
    pub data_directory: String,
    pub codex_home: String,
    pub config_file: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportConfigDiagnostics {
    pub valid: bool,
    pub active_provider: Option<String>,
    pub managed_provider_present: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportConnectionDiagnostics {
    pub active_kind: String,
    pub provider_count: usize,
    pub official_account_count: usize,
    pub active_model: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportStorageDiagnostics {
    pub files: Vec<SupportFileDiagnostics>,
    pub usage_database: SupportUsageDatabaseDiagnostics,
    pub session_database_count: usize,
    pub indexed_session_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportFileDiagnostics {
    pub name: String,
    pub exists: bool,
    pub readable: bool,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportUsageDatabaseDiagnostics {
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub schema_version: Option<i64>,
    pub quick_check: String,
    pub event_count: Option<u64>,
    pub cursor_count: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportNetworkDiagnostics {
    pub environment_proxy_configured: bool,
    pub no_proxy_configured: bool,
    pub system_proxy_configured: bool,
    pub tls_backend: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportPrivacyDiagnostics {
    pub home_paths_redacted: bool,
    pub omitted: Vec<String>,
}

fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), AppError> {
    if headers.len() > MAX_CUSTOM_HEADERS {
        return Err(AppError::InvalidConfig(
            "自定义请求头不能超过 64 项。".into(),
        ));
    }
    for (name, value) in headers {
        ensure_char_limit(
            name,
            MAX_HEADER_NAME_CHARS,
            "自定义请求头名称不能超过 256 个字符。",
        )?;
        ensure_char_limit(
            value,
            MAX_HEADER_VALUE_CHARS,
            "单个自定义请求头的值不能超过 8,192 个字符。",
        )?;
        reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| AppError::InvalidConfig(format!("自定义请求头名称无效：{name}。")))?;
        reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| AppError::InvalidConfig(format!("自定义请求头的值无效：{name}。")))?;
    }
    Ok(())
}

pub(crate) fn ensure_char_limit(
    value: &str,
    limit: usize,
    message: &'static str,
) -> Result<(), AppError> {
    if value.chars().count() > limit {
        return Err(AppError::InvalidConfig(message.into()));
    }
    Ok(())
}

pub(crate) fn is_personal_access_token_credential(credential: &CodexAuthCredential) -> bool {
    credential.auth_mode == "personal_access_token"
        || (credential.tokens.id_token.trim().is_empty()
            && credential.tokens.refresh_token.trim().is_empty()
            && credential.tokens.access_token.trim().starts_with("at-"))
}

pub(crate) fn validate_official_credential(
    credential: &CodexAuthCredential,
) -> Result<(), AppError> {
    let tokens = &credential.tokens;
    let personal_access_token = is_personal_access_token_credential(credential);
    if !matches!(
        credential.auth_mode.as_str(),
        "chatgpt" | "personal_access_token"
    ) || tokens.access_token.trim().is_empty()
        || tokens.account_id.trim().is_empty()
        || (!personal_access_token && credential.last_refresh.trim().is_empty())
    {
        return Err(AppError::InvalidConfig(
            "保存的 OpenAI 登录凭据不完整，请重新进行官方授权或导入 Cookie 数据。".into(),
        ));
    }
    for token in [
        tokens.id_token.as_str(),
        tokens.access_token.as_str(),
        tokens.refresh_token.as_str(),
    ] {
        ensure_char_limit(
            token,
            MAX_CREDENTIAL_CHARS,
            "OpenAI 登录凭据过长，请重新进行官方授权或检查 Cookie 数据。",
        )?;
    }
    ensure_char_limit(
        &tokens.account_id,
        MAX_ACCOUNT_ID_CHARS,
        "OpenAI 账号标识不能超过 512 个字符。",
    )?;
    Ok(())
}

fn redact_header_values(headers: &mut BTreeMap<String, String>) {
    for value in headers.values_mut() {
        value.clear();
    }
}

fn default_timeout_secs() -> u64 {
    30
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_save_input_ignores_backend_managed_model_fields() {
        let input: ProviderSaveInput = serde_json::from_value(serde_json::json!({
            "id": "provider",
            "name": "Provider",
            "baseUrl": "https://example.test/v1",
            "headers": {"x-tenant": "tenant"},
            "timeoutSecs": 45,
            "enabled": true,
            "apiType": "chat",
            "apiKey": "secret",
            "model": "injected-model",
            "availableModels": ["injected-model"],
            "modelContextWindows": {"injected-model": 999999},
            "contextWindowOverride": 262144,
            "modelsDevMeta": {"injected-model": {"name": "Injected"}},
            "active": true,
            "hasApiKey": true,
            "createdAt": 1,
            "updatedAt": 2
        }))
        .unwrap();

        let profile = ProviderProfile::from(input);

        assert!(profile.model.is_empty());
        assert!(profile.available_models.is_empty());
        assert!(profile.model_context_windows.is_empty());
        assert_eq!(profile.context_window_override, Some(262_144));
        assert!(profile.models_dev_meta.is_empty());
        assert!(!profile.active);
        assert!(!profile.has_api_key);
        assert_eq!(profile.created_at, 0);
        assert_eq!(profile.updated_at, 0);
        assert_eq!(profile.api_type, ProviderApiType::Chat);
    }

    #[test]
    fn redacted_provider_never_serializes_api_key() {
        let provider = ProviderProfile {
            id: "provider".into(),
            name: "provider".into(),
            base_url: "https://example.test/v1".into(),
            headers: BTreeMap::from([("x-private-token".into(), "header-secret".into())]),
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
            api_key: Some("upstream-secret".into()),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
        .redacted();
        let serialized = serde_json::to_string(&provider).unwrap();
        assert!(!serialized.contains("upstream-secret"));
        assert!(!serialized.contains("header-secret"));
        assert_eq!(provider.api_key, None);
    }

    #[test]
    fn provider_url_requires_https_except_for_loopback() {
        let make = |base_url: &str| {
            serde_json::from_value::<ProviderProfile>(serde_json::json!({
                "name": "provider",
                "baseUrl": base_url,
                "contextWindow": null,
                "autoCompactThreshold": null,
                "activeAccountId": null
            }))
            .unwrap()
        };

        assert!(
            make("http://api.example.test/v1")
                .normalize_and_validate()
                .is_err()
        );
        assert!(
            make("https://api.example.test/v1?token=secret")
                .normalize_and_validate()
                .is_err()
        );
        assert!(
            make("http://127.0.0.1:9000/v1")
                .normalize_and_validate()
                .is_ok()
        );
    }

    #[test]
    fn provider_context_window_override_must_be_positive() {
        let mut provider: ProviderProfile = serde_json::from_value(serde_json::json!({
            "name": "provider",
            "baseUrl": "https://example.test/v1",
            "contextWindowOverride": 0
        }))
        .unwrap();
        assert!(provider.normalize_and_validate().is_err());

        provider.context_window_override = Some(128_000);
        assert!(provider.normalize_and_validate().is_ok());
        provider.context_window_override = Some(MAX_CONTEXT_WINDOW_OVERRIDE + 1);
        assert!(provider.normalize_and_validate().is_err());
    }

    #[test]
    fn provider_and_api_key_inputs_have_size_limits() {
        let mut provider = ProviderProfile {
            id: String::new(),
            name: "x".repeat(MAX_DISPLAY_NAME_CHARS + 1),
            base_url: "https://example.test/v1".into(),
            headers: BTreeMap::new(),
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
            api_key: Some("x".repeat(MAX_API_KEY_CHARS + 1)),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        };
        assert!(provider.normalize_and_validate().is_err());

        let mut oversized = ProviderProfile {
            id: String::new(),
            name: "key".into(),
            base_url: "https://example.test/v1".into(),
            headers: BTreeMap::new(),
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
            api_key: Some("x".repeat(MAX_API_KEY_CHARS + 1)),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        };
        assert!(oversized.normalize_and_validate().is_err());
    }

    fn official_account() -> StoredOfficialAccount {
        StoredOfficialAccount {
            id: "local-account".into(),
            name: "OpenAI".into(),
            remark: "日常开发".into(),
            account_id: "workspace-account".into(),
            email: "person@example.test".into(),
            credential: CodexAuthCredential {
                auth_mode: "chatgpt".into(),
                openai_api_key: None,
                tokens: CodexAuthTokens {
                    id_token: "secret-id-token".into(),
                    access_token: "secret-access-token".into(),
                    refresh_token: "secret-refresh-token".into(),
                    account_id: "workspace-account".into(),
                },
                last_refresh: "2026-07-14T00:00:00Z".into(),
            },
            source: OfficialAccountSource::OpenAiOauth,
            expires_at: Some(1_800_000_000),
            quota: ProviderAccountQuota::default(),
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn official_view_and_device_dtos_never_serialize_credentials() {
        let view = official_account()
            .view_with_credential_refresh(true, CredentialRefreshState::default());
        assert_eq!(serde_json::to_value(&view).unwrap()["remark"], "日常开发");
        let values = [
            serde_json::to_value(&view).unwrap(),
            serde_json::to_value(OpenAiDeviceAuthorization {
                operation_id: "operation".into(),
                user_code: "ABCD-EFGH".into(),
                verification_uri: "https://auth.openai.com/device".into(),
                expires_at: 1_800_000_000,
                interval_secs: 5,
            })
            .unwrap(),
            serde_json::to_value(OpenAiDevicePoll::Complete {
                account: Box::new(view),
            })
            .unwrap(),
        ];

        for value in values {
            let serialized = value.to_string();
            assert!(!serialized.contains("secret-id-token"));
            assert!(!serialized.contains("secret-access-token"));
            assert!(!serialized.contains("secret-refresh-token"));
            assert!(!serialized.contains("\"credential\":"));
            assert!(!serialized.contains("idToken"));
            assert!(!serialized.contains("accessToken"));
            assert!(!serialized.contains("refreshToken"));
            assert!(!serialized.contains("id_token"));
            assert!(!serialized.contains("access_token"));
            assert!(!serialized.contains("refresh_token"));
            assert!(!serialized.contains("device_auth_id"));
        }
    }

    #[test]
    fn stored_official_account_defaults_remark_for_legacy_data() {
        let mut value = serde_json::to_value(official_account()).unwrap();
        value.as_object_mut().unwrap().remove("remark");

        let restored: StoredOfficialAccount = serde_json::from_value(value).unwrap();

        assert_eq!(restored.remark, "");
        assert_eq!(restored.account_id, "workspace-account");
    }

    #[test]
    fn deleted_device_session_settings_are_ignored_as_legacy_data() {
        let restored: AppConfig = serde_json::from_value(serde_json::json!({
            "codex": {},
            "active": { "kind": "none" },
            "providers": [],
            "officialAccounts": [],
            "officialInstallationIdSettings": {
                "legacy": { "enabled": true, "installationId": "old", "sessionId": "old" }
            }
        }))
        .unwrap();
        assert!(
            serde_json::to_value(restored)
                .unwrap()
                .get("officialInstallationIdSettings")
                .is_none()
        );
    }

    #[test]
    fn credential_serialization_matches_codex_auth_json_shape() {
        let credential = official_account().credential;
        let value = serde_json::to_value(credential).unwrap();
        assert_eq!(value["auth_mode"], "chatgpt");
        assert!(value["OPENAI_API_KEY"].is_null());
        assert_eq!(value["tokens"]["account_id"], "workspace-account");
        assert_eq!(value["last_refresh"], "2026-07-14T00:00:00Z");
        assert!(value.get("authMode").is_none());
        assert!(value["tokens"].get("accessToken").is_none());
    }

    #[test]
    fn sensitive_account_debug_output_is_redacted() {
        let rendered = format!("{:?}", official_account());
        assert!(!rendered.contains("secret-id-token"));
        assert!(!rendered.contains("secret-access-token"));
        assert!(!rendered.contains("secret-refresh-token"));
        assert!(rendered.contains("[REDACTED]"));
    }

    fn credential() -> CodexAuthCredential {
        CodexAuthCredential {
            auth_mode: "chatgpt".into(),
            openai_api_key: None,
            tokens: CodexAuthTokens {
                id_token: "id-secret".into(),
                access_token: "access-secret".into(),
                refresh_token: "refresh-secret".into(),
                account_id: "workspace-account".into(),
            },
            last_refresh: "2026-07-14T00:00:00Z".into(),
        }
    }

    #[test]
    fn token_local_identity_renders_stable_lowercase_hex_suffix() {
        let first = token_local_identity("secret-token");
        assert!(first.starts_with("proxy-"));
        assert_eq!(first.len(), "proxy-".len() + 24);
        assert!(first[6..].chars().all(|c| c.is_ascii_hexdigit()));
        assert!(first[6..].chars().all(|c| !c.is_ascii_uppercase()));
        assert_eq!(first, token_local_identity("secret-token"));
        assert_ne!(first, token_local_identity("other-token"));
    }

    #[test]
    fn validate_official_credential_accepts_complete_oauth_login() {
        assert!(validate_official_credential(&credential()).is_ok());
    }

    #[test]
    fn validate_official_credential_rejects_incomplete_login() {
        let mut missing_access = credential();
        missing_access.tokens.access_token.clear();
        assert!(validate_official_credential(&missing_access).is_err());

        let mut missing_account = credential();
        missing_account.tokens.account_id.clear();
        assert!(validate_official_credential(&missing_account).is_err());

        let mut missing_refresh = credential();
        missing_refresh.tokens.refresh_token.clear();
        assert!(!is_personal_access_token_credential(&missing_refresh));
        assert!(validate_official_credential(&missing_refresh).is_ok());

        let mut missing_last_refresh = credential();
        missing_last_refresh.last_refresh.clear();
        assert!(!is_personal_access_token_credential(&missing_last_refresh));
        assert!(validate_official_credential(&missing_last_refresh).is_err());

        let mut wrong_mode = credential();
        wrong_mode.auth_mode = "azure".into();
        assert!(validate_official_credential(&wrong_mode).is_err());
    }

    #[test]
    fn validate_official_credential_accepts_personal_access_token_without_session_tokens() {
        let credential = CodexAuthCredential {
            tokens: CodexAuthTokens {
                id_token: String::new(),
                access_token: "at-pat-secret".into(),
                refresh_token: String::new(),
                account_id: "workspace-account".into(),
            },
            last_refresh: String::new(),
            ..credential()
        };
        assert!(is_personal_access_token_credential(&credential));
        assert!(validate_official_credential(&credential).is_ok());
    }

    #[test]
    fn access_token_only_oauth_is_not_treated_as_personal_access_token() {
        let credential = CodexAuthCredential {
            tokens: CodexAuthTokens {
                id_token: String::new(),
                access_token: "header.payload.signature".into(),
                refresh_token: String::new(),
                account_id: "workspace-account".into(),
            },
            ..credential()
        };

        assert!(!is_personal_access_token_credential(&credential));
        assert!(validate_official_credential(&credential).is_ok());
    }

    #[test]
    fn oauth_with_id_token_and_empty_refresh_is_not_personal_access_token() {
        let mut credential = credential();
        credential.tokens.refresh_token.clear();

        assert!(!is_personal_access_token_credential(&credential));
        assert!(validate_official_credential(&credential).is_ok());
    }

    #[test]
    fn validate_official_credential_rejects_oversized_tokens() {
        let mut oversized = credential();
        oversized.tokens.id_token = "x".repeat(MAX_CREDENTIAL_CHARS + 1);
        let error = validate_official_credential(&oversized).unwrap_err();
        assert!(error.to_string().contains("凭据过长"));

        let mut oversized_account = credential();
        oversized_account.tokens.account_id = "a".repeat(MAX_ACCOUNT_ID_CHARS + 1);
        let error = validate_official_credential(&oversized_account).unwrap_err();
        assert!(error.to_string().contains("不能超过 512 个字符"));
    }
}
