use crate::{
    activation::{
        activate_openai_record_with_paths, ensure_codex_stopped, sync_active_openai_credential,
    },
    auth_center::{AuthCenter, DevicePollResult, ImportExchangeError},
    chat_proxy::ChatProxyRegistry,
    codex::{self, ConfigManager},
    local_usage::UsageLedger,
    models::*,
    official_quota, official_reset_credits, proxy_import, record_current_activation,
    session_index::SessionIndex,
    state::{ActivationLock, ApiClient, ResetCreditOperations},
    storage::{RotatedOfficialImportError, Store},
};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Serialize;
use tauri::State;

const QUOTA_REFRESH_CONCURRENCY: usize = 4;

const FIVE_HOURS_SECONDS: i64 = 18_000;
const SEVEN_DAYS_SECONDS: i64 = 604_800;

/// 以服务返回的时长为准；旧响应缺少时长时才按 primary/secondary 兼容。
/// primary 不存在时绝不凭空构造 5H 窗口。
fn estimate_windows(quota: &ProviderAccountQuota) -> Vec<(i64, i64, f64)> {
    let Some(QuotaData::Windowed { primary, secondary }) = quota.data.as_ref() else {
        return vec![];
    };
    [
        (primary.as_ref(), Some(FIVE_HOURS_SECONDS)),
        (secondary.as_ref(), Some(SEVEN_DAYS_SECONDS)),
    ]
    .into_iter()
    .filter_map(|(window, fallback_seconds)| {
        let window = window?;
        let window_seconds = window.window_seconds.or(fallback_seconds)?;
        let reset_at = window.reset_at?;
        (window_seconds > 0 && reset_at > 0).then_some((
            window_seconds,
            reset_at,
            window.used_percent,
        ))
    })
    .fold(Vec::new(), |mut windows, window| {
        if !windows
            .iter()
            .any(|(seconds, reset_at, _)| *seconds == window.0 && *reset_at == window.1)
        {
            windows.push(window);
        }
        windows
    })
}

fn estimate_window_ids(quota: &ProviderAccountQuota) -> Vec<(i64, i64)> {
    estimate_windows(quota)
        .into_iter()
        .map(|(window_seconds, reset_at, _)| (window_seconds, reset_at))
        .collect()
}

fn clear_estimates_for_current_quota(
    store: &Store,
    account_id: &str,
    quota: &ProviderAccountQuota,
) -> Result<(), AppError> {
    store.clear_official_account_quota_estimates(account_id, &estimate_window_ids(quota))
}

fn quota_estimation_crossed_reset(results: &[QuotaEstimateWindowResult], now: i64) -> bool {
    results.iter().any(|result| now >= result.reset_at)
}

fn ensure_current_activation(activation: &ActivationLock, operation: u64) -> Result<(), AppError> {
    if activation.is_current(operation) {
        Ok(())
    } else {
        Err(AppError::StaleOperation)
    }
}

fn apply_successful_quota_snapshot(
    snapshot: &mut ProviderAccountQuota,
    data: QuotaData,
    plan_type: Option<String>,
    reset_credits: ResetCreditSummary,
    now: i64,
) {
    snapshot.status = QuotaStatus::Success;
    snapshot.data = Some(data);
    snapshot.plan_type = plan_type;
    snapshot.fetched_at = Some(now);
    snapshot.error = None;
    snapshot.error_code = None;
    snapshot.reset_credits = reset_credits;
    // 新额度快照的百分比不能与旧快照的金额混用；同周期旧估算也必须重新计算。
    let current_windows = estimate_window_ids(snapshot);
    snapshot.estimates.retain(|estimate| {
        !current_windows.iter().any(|(window_seconds, reset_at)| {
            estimate.window_seconds == *window_seconds && estimate.reset_at == *reset_at
        })
    });
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyImportResult {
    pub accounts: Vec<OfficialAccountView>,
    pub saved_count: usize,
    pub partial: Option<PartialImport>,
    pub detected_formats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartialImport {
    message: String,
    retry_from: usize,
}

#[tauri::command]
pub(crate) async fn connections_import_cookie(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    activation: State<'_, ActivationLock>,
    name: Option<String>,
    name_offset: Option<u32>,
    account_id: Option<String>,
    content: String,
) -> Result<ProxyImportResult, AppError> {
    let name = name
        .as_deref()
        .map(|label| (label, name_offset.unwrap_or(0)));
    connections_import_cookie_in_store(&store, &center, &activation, name, account_id, content)
        .await
}

async fn connections_import_cookie_in_store(
    store: &Store,
    center: &AuthCenter,
    activation: &ActivationLock,
    name: Option<(&str, u32)>,
    account_id: Option<String>,
    content: String,
) -> Result<ProxyImportResult, AppError> {
    let mut imported =
        proxy_import::parse_proxy_credentials(&content).map_err(AppError::InvalidConfig)?;
    if let Some(account_id) = account_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if imported.len() != 1 {
            return Err(AppError::InvalidConfig(
                "导入内容包含多个账号时，请不要填写统一的 Account ID；每个账号应在 JSON 中提供自己的标识。"
                    .into(),
            ));
        }
        imported[0].account_id = Some(account_id.to_owned());
    }
    let _guard = activation.0.lock().await;
    let home = codex::home(&store.codex_home_setting()?);
    sync_active_openai_credential(store, &home)?;
    let detected_formats = imported
        .iter()
        .map(|item| item.source_format.label().to_string())
        .fold(Vec::<String>::new(), |mut formats, format| {
            if !formats.contains(&format) {
                formats.push(format);
            }
            formats
        });
    let (saved, mut partial) = import_credential_rows(store, imported, name, |credential, name| {
        center.exchange_imported_credential(credential, name)
    })
    .await?;
    let views = if saved.is_empty() {
        Ok(vec![])
    } else {
        store.saved_account_views(&saved)
    };
    let accounts = match views {
        Ok(views) => views,
        Err(error) => {
            let recovery = partial.get_or_insert_with(|| PartialImport {
                message: format!("已保存 {} 项。", saved.len()),
                retry_from: saved.len(),
            });
            recovery
                .message
                .push_str(&format!(" 无法读取账号列表：{error}"));
            Vec::new()
        }
    };
    Ok(ProxyImportResult {
        accounts,
        saved_count: saved.len(),
        detected_formats,
        partial,
    })
}

async fn import_credential_rows<F, Fut>(
    store: &Store,
    rows: Vec<proxy_import::ImportedProxyCredential>,
    name: Option<(&str, u32)>,
    mut exchange: F,
) -> Result<(Vec<StoredOfficialAccount>, Option<PartialImport>), AppError>
where
    F: FnMut(proxy_import::ImportedProxyCredential, Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<StoredOfficialAccount, ImportExchangeError>>,
{
    let total = rows.len();
    let mut local = Vec::new();
    let mut exchanges = Vec::new();
    let mut seen = std::collections::HashMap::new();
    for (index, row) in rows.into_iter().enumerate() {
        let name = name.map(|(name, offset)| {
            if total == 1 && offset == 0 {
                name.to_owned()
            } else {
                format!("{name} #{}", u64::from(offset) + index as u64 + 1)
            }
        });
        crate::auth_center::validate_import_fields(&row, name.as_deref())?;
        let rotates = row.access_token.is_none();
        if let Some(token) = row.refresh_token.as_ref() {
            if seen
                .insert(token.clone(), rotates)
                .is_some_and(|previous| previous || rotates)
            {
                return Err(AppError::InvalidConfig(format!(
                    "第 {} 项重复使用同一个待换发凭据，请移除重复项。",
                    index + 1
                )));
            }
        }
        if rotates {
            exchanges.push((index, row, name));
        } else {
            local.push(crate::auth_center::account_from_imported_tokens(row, name)?);
        }
    }
    if exchanges.is_empty() {
        return Ok((store.save_official_accounts(&local)?, None));
    }
    store.retry_pending_official_import()?;
    store.ensure_official_account_capacity(&local, exchanges.len())?;
    let mut local = local.into_iter();
    let mut saved = Vec::with_capacity(total);
    for (index, row, name) in exchanges {
        let preceding: Vec<_> = local.by_ref().take(index - saved.len()).collect();
        if !preceding.is_empty() {
            match store.save_official_accounts(&preceding) {
                Ok(records) => saved.extend(records),
                Err(error) => {
                    let count = saved.len();
                    return Ok((
                        saved,
                        Some(PartialImport {
                            retry_from: count,
                            message: format!("前 {count} 项已保存，后续账号保存失败：{error}"),
                        }),
                    ));
                }
            }
        }
        let refresh_token = row
            .refresh_token
            .as_deref()
            .ok_or_else(|| AppError::InvalidConfig("Cookie 登录数据缺少 Refresh Token。".into()))?;
        let fingerprint = match store.record_import_refresh_attempt(refresh_token) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                let previous_attempt = matches!(error, AppError::InvalidConfig(_));
                return Ok((
                    saved,
                    Some(PartialImport {
                        retry_from: index + usize::from(previous_attempt),
                        message: format!("第 {} 项未提交，前 {index} 项已保存。{error}", index + 1),
                    }),
                ));
            }
        };
        let rotated = match exchange(row, name).await {
            Ok(rotated) => rotated,
            Err(ImportExchangeError::MayHaveRotated) => {
                return Ok((
                    saved,
                    Some(PartialImport {
                        retry_from: index + 1,
                        message: format!(
                            "第 {} 项的换发结果无法确认，请为该账号重新授权。前 {index} 项已保存。",
                            index + 1
                        ),
                    }),
                ));
            }
            Err(ImportExchangeError::Rejected(error)) => {
                let error = store
                    .clear_import_refresh_attempt(&fingerprint)
                    .err()
                    .unwrap_or(error);
                return Ok((
                    saved,
                    Some(PartialImport {
                        retry_from: index,
                        message: format!(
                            "第 {} 项导入失败，前 {index} 项已保存。请核对剩余凭据后重试：{error}",
                            index + 1
                        ),
                    }),
                ));
            }
        };
        let record = match store.save_rotated_official_import(rotated, Some(fingerprint)) {
            Ok(record) => record,
            Err(error) => {
                let action = match error {
                    RotatedOfficialImportError::Pending { on_disk: true } => {
                        "新凭据已暂存。恢复数据目录写入后，刷新账号列表即可补存。".into()
                    }
                    RotatedOfficialImportError::Pending { on_disk: false } => {
                        "新凭据暂存在当前应用。请保持应用打开，恢复数据目录写入后刷新账号列表。"
                            .into()
                    }
                    RotatedOfficialImportError::Invalid(error) => {
                        format!("新凭据无法保存，请为该账号重新授权：{error}")
                    }
                };
                return Ok((
                    saved,
                    Some(PartialImport {
                        retry_from: index + 1,
                        message: format!(
                            "第 {} 项凭据已换发。{action} 前 {index} 项已保存。",
                            index + 1
                        ),
                    }),
                ));
            }
        };
        saved.push(record);
    }
    if local.len() > 0 {
        let remaining: Vec<_> = local.collect();
        match store.save_official_accounts(&remaining) {
            Ok(records) => saved.extend(records),
            Err(error) => {
                let count = saved.len();
                return Ok((
                    saved,
                    Some(PartialImport {
                        retry_from: count,
                        message: format!("前 {count} 项已保存，其余账号保存失败：{error}"),
                    }),
                ));
            }
        }
    }
    Ok((saved, None))
}

#[tauri::command]
pub(crate) async fn connections_update_account_remark(
    store: State<'_, Store>,
    id: String,
    remark: String,
) -> Result<OfficialAccountView, AppError> {
    let saved = store.update_official_account_remark(&id, remark)?;
    store.official_account_view(&saved.id)
}

#[tauri::command]
pub(crate) async fn connections_update_account_remarks(
    store: State<'_, Store>,
    updates: Vec<AccountRemarkUpdate>,
) -> Result<Vec<OfficialAccountView>, AppError> {
    connections_update_account_remarks_in_store(&store, updates)
}

fn connections_update_account_remarks_in_store(
    store: &Store,
    updates: Vec<AccountRemarkUpdate>,
) -> Result<Vec<OfficialAccountView>, AppError> {
    let saved = store.update_official_account_remarks(updates)?;
    store.saved_account_views(&saved)
}

#[tauri::command]
pub(crate) async fn connections_refresh_login(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    id: String,
) -> Result<CredentialMaintenanceResult, AppError> {
    connections_refresh_login_in_store(&store, &center, &client, &manager, &activation, &proxy, &id)
        .await
}

async fn connections_refresh_login_in_store(
    store: &Store,
    center: &AuthCenter,
    _client: &ApiClient,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    id: &str,
) -> Result<CredentialMaintenanceResult, AppError> {
    let result = crate::credential_maintenance::maintain_login(
        store, center, manager, activation, proxy, id,
    )
    .await?;
    // 在线检查只复用现有安全额度请求，并单独存储结论；不会把本地刷新状态伪装成登录有效。
    if !matches!(
        result.outcome,
        CredentialMaintenanceOutcome::WaitingRetry
            | CredentialMaintenanceOutcome::ReauthenticationRequired
    ) {
        let quota = refresh_official_quota(store, center, _client, activation, id).await?;
        crate::credential_maintenance::record_login_verification(store, id, &quota)?;
    }
    Ok(CredentialMaintenanceResult {
        account: store.official_account_view(id)?,
        outcome: result.outcome,
    })
}

async fn refresh_official_quota(
    store: &Store,
    center: &AuthCenter,
    client: &ApiClient,
    activation: &ActivationLock,
    account_id: &str,
) -> Result<ProviderAccountQuota, AppError> {
    let snapshot = fetch_official_quota(store, center, client, activation, account_id).await?;
    store.save_official_account_quota(account_id, snapshot.clone())
}

async fn fetch_official_quota(
    store: &Store,
    center: &AuthCenter,
    client: &ApiClient,
    activation: &ActivationLock,
    account_id: &str,
) -> Result<ProviderAccountQuota, AppError> {
    let _quota_guard = client.1.lock().await;
    let stored = store.official_account(account_id)?;
    let now = chrono::Utc::now().timestamp();
    let mut snapshot = stored.quota.clone();
    snapshot.last_attempt_at = Some(now);
    let account = match account_for_quota(store, center, activation, account_id).await {
        Ok(account) => account,
        Err(error) => {
            snapshot.status = match error {
                AppError::InvalidConfig(_) => QuotaStatus::Unauthorized,
                AppError::StaleOperation | AppError::Internal(_) => QuotaStatus::Error,
            };
            snapshot.error = Some(error.to_string());
            snapshot.error_code = None;
            return Ok(snapshot);
        }
    };
    let http = client.current()?;
    let mut quota_result = official_quota::fetch_quota(&http, &account).await;
    if matches!(&quota_result, Err(error) if error.is_retryable()) {
        client.invalidate();
        let retry_http = client.current()?;
        quota_result = official_quota::fetch_quota(&retry_http, &account).await;
    }
    match quota_result {
        Ok(data) => {
            apply_successful_quota_snapshot(
                &mut snapshot,
                data.data,
                data.plan_type,
                data.reset_credits,
                chrono::Utc::now().timestamp(),
            );
        }
        Err(error) => {
            snapshot.status = error.status;
            snapshot.error = Some(error.message);
            snapshot.error_code = error.code;
        }
    }
    Ok(snapshot)
}

async fn account_for_quota(
    store: &Store,
    center: &AuthCenter,
    activation: &ActivationLock,
    account_id: &str,
) -> Result<StoredOfficialAccount, AppError> {
    let _guard = activation.0.lock().await;
    let stored = store.official_account(account_id)?;
    let is_active = store.read(|state| {
        matches!(state.active.kind, ActiveKind::Official)
            && state.active.account_id.as_deref() == Some(account_id)
    })?;
    // “刷新额度”对当前账号保持只读：不得轮换正在被 Codex 使用的凭据。
    if is_active {
        Ok(stored)
    } else {
        center.refresh_account(store, account_id).await
    }
}

async fn load_reset_credits(
    store: &Store,
    center: &AuthCenter,
    client: &ApiClient,
    activation: &ActivationLock,
    account_id: &str,
) -> Result<ResetCreditDetails, AppError> {
    // 和整体额度快照使用同一网络锁：详情摘要仅字段级保存，避免较旧快照覆盖它。
    let _network_guard = client.1.lock().await;
    let account = account_for_quota(store, center, activation, account_id).await?;
    let http = client.current()?;
    let details = official_reset_credits::fetch_reset_credits(&http, &account)
        .await
        .map_err(|error| AppError::InvalidConfig(error.message))?;
    store.save_official_account_reset_credit_summary(account_id, details.summary.clone())?;
    Ok(details)
}

fn credit_is_usable(credit: &ResetCredit, now: i64) -> bool {
    credit.status.as_deref() == Some("available")
        && credit.expires_at.is_none_or(|expires_at| expires_at > now)
}

fn validate_idempotency_key(value: &str) -> Result<(), AppError> {
    let value = value.trim();
    if !(16..=256).contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AppError::InvalidConfig(
            "重置卡请求标识无效；请关闭确认框后重新开始一次操作。".into(),
        ));
    }
    Ok(())
}

#[tauri::command]
pub(crate) async fn connections_get_reset_credits(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    activation: State<'_, ActivationLock>,
    account_id: String,
) -> Result<ResetCreditDetails, AppError> {
    load_reset_credits(&store, &center, &client, &activation, &account_id).await
}

#[derive(Debug, Serialize)]
#[serde(tag = "stage", content = "message", rename_all = "snake_case")]
pub(crate) enum ResetCreditError {
    NotSubmitted(String),
    Unknown(String),
}

impl From<AppError> for ResetCreditError {
    fn from(value: AppError) -> Self {
        Self::NotSubmitted(value.to_string())
    }
}

#[tauri::command]
#[allow(clippy::too_many_arguments)] // Tauri injects each managed state separately.
pub(crate) async fn connections_consume_reset_credit(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    activation: State<'_, ActivationLock>,
    operations: State<'_, ResetCreditOperations>,
    account_id: String,
    credit_id: String,
    idempotency_key: String,
) -> Result<ResetCreditConsumeResult, ResetCreditError> {
    validate_idempotency_key(&idempotency_key)?;
    let _operation_guard = operations.lock(&account_id).await;
    if operations.is_unknown(&account_id, &credit_id).await {
        return Err(ResetCreditError::Unknown(
            "该重置卡此前结果未知；为避免重复消费，本会话不会再次提交。".into(),
        ));
    }
    // 先从该账号自己的服务端详情确认归属和可用性；不依据客户端传入的标题或数量。
    let account = account_for_quota(&store, &center, &activation, &account_id).await?;
    let outcome = {
        let _network_guard = client.1.lock().await;
        let http = client.current()?;
        submit_verified_credit(
            &operations,
            &account_id,
            &credit_id,
            official_reset_credits::fetch_reset_credits(&http, &account).await,
            official_reset_credits::consume_reset_credit(
                &http,
                &account,
                &credit_id,
                &idempotency_key,
            ),
        )
        .await?
    };

    let mut refresh_errors = Vec::new();
    if matches!(
        outcome,
        ResetCreditConsumeOutcome::Reset | ResetCreditConsumeOutcome::AlreadyRedeemed
    ) {
        match store.official_account(&account_id) {
            Ok(account) => {
                if let Err(error) =
                    clear_estimates_for_current_quota(&store, &account_id, &account.quota)
                {
                    refresh_errors.push(format!("旧额度估算清理失败：{error}"));
                }
            }
            Err(error) => refresh_errors.push(format!("旧额度估算读取失败：{error}")),
        }
    }

    // 消费已由服务端确认；后续详情或额度刷新失败只能作为附带告警，不能改写消费结果。
    let details = match load_reset_credits(&store, &center, &client, &activation, &account_id).await
    {
        Ok(details) => details,
        Err(error) => {
            refresh_errors.push(format!("重置卡详情刷新失败：{error}"));
            ResetCreditDetails {
                account_id: account_id.clone(),
                summary: ResetCreditSummary::default(),
                credits: Vec::new(),
            }
        }
    };
    let quota =
        match refresh_official_quota(&store, &center, &client, &activation, &account_id).await {
            Ok(quota) if quota.status == QuotaStatus::Success => Some(quota),
            Ok(quota) => {
                refresh_errors.push(
                    quota
                        .error
                        .clone()
                        .unwrap_or_else(|| "额度刷新未成功。".into()),
                );
                Some(quota)
            }
            Err(error) => {
                refresh_errors.push(format!("额度刷新失败：{error}"));
                None
            }
        };
    Ok(ResetCreditConsumeResult {
        outcome,
        details,
        quota,
        refresh_error: (!refresh_errors.is_empty()).then(|| refresh_errors.join("；")),
    })
}

async fn submit_verified_credit(
    operations: &ResetCreditOperations,
    account_id: &str,
    credit_id: &str,
    before: Result<ResetCreditDetails, official_quota::QuotaFetchError>,
    submit: impl std::future::Future<
        Output = Result<ResetCreditConsumeOutcome, official_quota::QuotaFetchError>,
    >,
) -> Result<ResetCreditConsumeOutcome, ResetCreditError> {
    let before = before.map_err(|error| ResetCreditError::NotSubmitted(error.message))?;
    let credit = before
        .credits
        .iter()
        .find(|credit| credit.id == credit_id)
        .ok_or_else(|| {
            AppError::InvalidConfig("重置卡不存在、已不属于该账号，或详情尚未完整提供该卡。".into())
        })?;
    if !credit_is_usable(credit, chrono::Utc::now().timestamp()) {
        return Err(ResetCreditError::NotSubmitted(
            "该重置卡已过期、正在使用、已使用或状态未知，不能再次提交。".into(),
        ));
    }
    // Mark before awaiting the POST so cancellation also leaves an uncertain operation locked.
    operations.mark_unknown(account_id, credit_id).await;
    let outcome = submit
        .await
        .map_err(|error| ResetCreditError::Unknown(error.message))?;
    if outcome != ResetCreditConsumeOutcome::Unknown {
        operations.clear_unknown(account_id, credit_id).await;
    }
    Ok(outcome)
}

#[tauri::command]
pub(crate) async fn connections_refresh_quota(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    activation: State<'_, ActivationLock>,
    account_id: String,
) -> Result<ProviderAccountQuota, AppError> {
    refresh_official_quota(&store, &center, &client, &activation, &account_id).await
}

#[tauri::command]
pub(crate) async fn connections_estimate_quota(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    activation: State<'_, ActivationLock>,
    ledger: State<'_, UsageLedger>,
    account_id: String,
) -> Result<QuotaEstimateResult, AppError> {
    let previous_quota = store.official_account(&account_id)?.quota;
    // 本轮开始即撤销当前窗口旧金额：后续的额度、扫描或完整性失败都不能继续展示它。
    clear_estimates_for_current_quota(&store, &account_id, &previous_quota)?;

    for attempt in 0..2 {
        let quota =
            refresh_official_quota(&store, &center, &client, &activation, &account_id).await?;
        if quota.status != QuotaStatus::Success {
            return Err(AppError::InvalidConfig(
                quota
                    .error
                    .unwrap_or_else(|| "刷新额度失败，暂无法估算。".into()),
            ));
        }
        let windows = estimate_windows(&quota);
        if windows.is_empty() {
            return Err(AppError::InvalidConfig(
                "当前额度窗口缺少可识别的时长或重置时间，暂无法估算。".into(),
            ));
        }
        // 本次网络快照可能已进入新周期；无论之后扫描或完整性校验是否失败，都不能展示其旧金额。
        clear_estimates_for_current_quota(&store, &account_id, &quota)?;

        let quota_snapshot_at_ms = quota
            .fetched_at
            .ok_or_else(|| AppError::Internal("刷新额度后缺少快照时间，无法安全估算。".into()))?
            .saturating_mul(1_000);
        let account = store.official_account(&account_id)?;
        let canonical_account_id = canonical_official_account_id(&account);
        // 对账必须发生在增量解析之前，确保本次新增事件可归属到当前官方账号。
        record_current_activation(&store, &ledger)?;
        let official_account_identities = store.read(|state| {
            state
                .official_accounts
                .iter()
                .map(|account| (account.id.clone(), canonical_official_account_id(account)))
                .collect::<Vec<_>>()
        })?;
        ledger.sync_official_account_identities(&official_account_identities)?;
        let codex_home = codex::home(&store.codex_home_setting()?);
        let refresh_started_at_ms = chrono::Utc::now().timestamp_millis();
        let ledger = ledger.inner().clone();
        let results = tokio::task::spawn_blocking(move || {
            ledger.refresh_and_estimate_account_quota(
                &codex_home,
                refresh_started_at_ms,
                &canonical_account_id,
                &windows,
                quota_snapshot_at_ms,
            )
        })
        .await
        .map_err(|error| AppError::Internal(error.to_string()))??;

        let crossed_reset =
            quota_estimation_crossed_reset(&results, chrono::Utc::now().timestamp());
        if crossed_reset {
            if attempt == 0 {
                continue;
            }
            return Err(AppError::InvalidConfig(
                "估算过程中额度周期再次重置，请稍后重新估算。".into(),
            ));
        }

        let successful = results
            .iter()
            .filter_map(|result| result.success.then_some(result.estimate.clone()).flatten())
            .collect::<Vec<_>>();
        if !successful.is_empty() {
            store.save_official_account_quota_estimates(&account_id, &successful)?;
        }
        return Ok(QuotaEstimateResult { windows: results });
    }

    Err(AppError::Internal("额度估算重试状态异常。".into()))
}

#[tauri::command]
pub(crate) async fn connections_refresh_all_quota(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    client: State<'_, ApiClient>,
    activation: State<'_, ActivationLock>,
) -> Result<Vec<QuotaRefreshResult>, AppError> {
    connections_refresh_all_quota_in_store(&store, &center, &client, &activation).await
}

async fn connections_refresh_all_quota_in_store(
    store: &Store,
    center: &AuthCenter,
    client: &ApiClient,
    activation: &ActivationLock,
) -> Result<Vec<QuotaRefreshResult>, AppError> {
    let account_ids = store.read(|state| {
        state
            .official_accounts
            .iter()
            .map(|account| account.id.clone())
            .collect::<Vec<_>>()
    })?;
    let requests = account_ids.into_iter().map(|account_id| async move {
        let quota = fetch_official_quota(store, center, client, activation, &account_id).await?;
        Ok::<_, AppError>((account_id, quota))
    });
    let fetched = stream::iter(requests)
        .buffered(QUOTA_REFRESH_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    store.save_official_account_quotas(&fetched)?;
    Ok(fetched
        .into_iter()
        .map(|(account_id, quota)| QuotaRefreshResult { quota, account_id })
        .collect())
}

#[tauri::command]
pub(crate) async fn connections_login_start(
    center: State<'_, AuthCenter>,
) -> Result<OpenAiDeviceAuthorization, AppError> {
    center.start_openai().await
}

#[tauri::command]
#[allow(clippy::too_many_arguments)] // Tauri injects each managed state separately.
pub(crate) async fn connections_login_poll(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    activation: State<'_, ActivationLock>,
    operation_id: String,
) -> Result<OpenAiDevicePoll, AppError> {
    match center.poll_openai(&operation_id).await? {
        DevicePollResult::Pending => Ok(OpenAiDevicePoll::Pending),
        DevicePollResult::Expired => Ok(OpenAiDevicePoll::Expired),
        DevicePollResult::Complete(account) => {
            let _guard = activation.0.lock().await;
            let saved = save_completed_login(&store, &account)?;
            center.ack_openai(&operation_id).await;
            Ok(OpenAiDevicePoll::Complete {
                account: Box::new(saved),
            })
        }
    }
}

fn save_completed_login(
    store: &Store,
    account: &StoredOfficialAccount,
) -> Result<OfficialAccountView, AppError> {
    sync_active_openai_credential(store, &codex::home(&store.codex_home_setting()?))?;
    let saved = store.save_official_account(account)?;
    store.official_account_view(&saved.id)
}

#[allow(clippy::too_many_arguments)]
async fn activate_resolved_official_account(
    store: &Store,
    center: &AuthCenter,
    manager: &ConfigManager,
    ledger: &UsageLedger,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    index: &SessionIndex,
    activation_operation: u64,
    id: &str,
    home: &std::path::Path,
) -> Result<RepairResult, AppError> {
    ensure_current_activation(activation, activation_operation)?;
    official_quota::ensure_account_usable(&store.official_account(id)?)?;
    ensure_codex_stopped(store)?;
    sync_active_openai_credential(store, home)?;
    let saved = center.refresh_account(store, id).await?;
    ensure_current_activation(activation, activation_operation)?;
    let (repair, affected_paths) = activate_openai_record_with_paths(
        store,
        manager,
        ledger,
        proxy,
        activation,
        activation_operation,
        &saved,
    )
    .await?;
    if let Err(error) = index.refresh_paths(home, &affected_paths) {
        index.invalidate();
        eprintln!("账号切换后定向刷新会话索引失败，已回退全量重建：{error}");
    }
    Ok(repair)
}

#[tauri::command]
#[allow(clippy::too_many_arguments)] // Tauri injects each managed state separately.
pub(crate) async fn connections_activate_account(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    manager: State<'_, ConfigManager>,
    ledger: State<'_, UsageLedger>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    index: State<'_, SessionIndex>,
    id: String,
) -> Result<RepairResult, AppError> {
    let activation_operation = activation.begin_operation();
    let _guard = activation.0.lock().await;
    let home = codex::home(&store.codex_home_setting()?);
    activate_resolved_official_account(
        &store,
        &center,
        &manager,
        &ledger,
        &activation,
        &proxy,
        &index,
        activation_operation,
        &id,
        &home,
    )
    .await
}

#[tauri::command]
#[allow(clippy::too_many_arguments)] // Tauri injects each managed state separately.
pub(crate) async fn connections_activate_official(
    store: State<'_, Store>,
    center: State<'_, AuthCenter>,
    manager: State<'_, ConfigManager>,
    ledger: State<'_, UsageLedger>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    index: State<'_, SessionIndex>,
) -> Result<RepairResult, AppError> {
    let activation_operation = activation.begin_operation();
    let _guard = activation.0.lock().await;
    ensure_current_activation(&activation, activation_operation)?;
    let (home_setting, id) = store.read(|state| {
        let id = state
            .active
            .account_id
            .clone()
            .filter(|_| matches!(state.active.kind, ActiveKind::Official))
            .or_else(|| {
                state
                    .official_accounts
                    .iter()
                    .max_by_key(|account| account.updated_at)
                    .map(|account| account.id.clone())
            });
        (state.codex.home.clone(), id)
    })?;
    let id =
        id.ok_or_else(|| AppError::InvalidConfig("请先在“账号与服务”中登录 OpenAI。".into()))?;
    let home = codex::home(&home_setting);
    activate_resolved_official_account(
        &store,
        &center,
        &manager,
        &ledger,
        &activation,
        &proxy,
        &index,
        activation_operation,
        &id,
        &home,
    )
    .await
}

#[tauri::command]
pub(crate) async fn connections_delete_account(
    store: State<'_, Store>,
    activation: State<'_, ActivationLock>,
    id: String,
) -> Result<(), AppError> {
    let _guard = activation.0.lock().await;
    store.delete_official_account(&id)
}

#[tauri::command]
pub(crate) async fn connections_delete_accounts(
    store: State<'_, Store>,
    activation: State<'_, ActivationLock>,
    ids: Vec<String>,
) -> Result<(), AppError> {
    connections_delete_accounts_in_store(&store, &activation, ids).await
}

async fn connections_delete_accounts_in_store(
    store: &Store,
    activation: &ActivationLock,
    ids: Vec<String>,
) -> Result<(), AppError> {
    let _guard = activation.0.lock().await;
    store.delete_official_accounts(ids)
}

#[tauri::command]
pub(crate) fn connections_open_login_page() -> Result<(), AppError> {
    platform_open()
}

fn platform_open() -> Result<(), AppError> {
    crate::platform::open_url("https://auth.openai.com/codex/device").map_err(|error| {
        AppError::Internal(format!(
            "无法自动打开 OpenAI 授权页面。请在浏览器中访问 https://auth.openai.com/codex/device：{error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::fs;

    #[tokio::test]
    async fn reset_preflight_can_retry_but_an_uncertain_post_stays_locked() {
        let operations = ResetCreditOperations::default();
        let network_error = || official_quota::QuotaFetchError::retryable("模拟超时");
        let details = || ResetCreditDetails {
            account_id: "account".into(),
            summary: Default::default(),
            credits: vec![
                serde_json::from_value(serde_json::json!({"id":"card","status":"available"}))
                    .unwrap(),
            ],
        };
        let first = submit_verified_credit(
            &operations,
            "account",
            "card",
            Err(network_error()),
            async { panic!("preflight must never send") },
        )
        .await
        .unwrap_err();
        assert_eq!(
            serde_json::to_value(first).unwrap()["stage"],
            "not_submitted"
        );
        assert!(!operations.is_unknown("account", "card").await);
        let error = submit_verified_credit(&operations, "account", "card", Ok(details()), async {
            Err(network_error())
        })
        .await
        .unwrap_err();
        assert_eq!(serde_json::to_value(error).unwrap()["stage"], "unknown");
        assert!(operations.is_unknown("account", "card").await);
    }

    #[test]
    fn stale_account_switch_is_rejected_after_an_async_boundary() {
        let activation = ActivationLock::default();
        let older = activation.begin_operation();
        let newer = activation.begin_operation();

        assert!(matches!(
            ensure_current_activation(&activation, older),
            Err(AppError::StaleOperation)
        ));
        assert!(ensure_current_activation(&activation, newer).is_ok());
    }

    fn oauth_account(account_id: &str, subject: &str, email: &str) -> StoredOfficialAccount {
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                "sub": subject,
                "chatgpt_account_id": account_id,
                "email": email
            })
            .to_string()
            .as_bytes(),
        );
        let token = format!("header.{payload}.signature");
        let mut account = account(account_id, "");
        account.name = email.into();
        account.email = email.into();
        account.source = OfficialAccountSource::OpenAiOauth;
        account.credential.tokens.id_token = token.clone();
        account.credential.tokens.access_token = token;
        account.credential.tokens.refresh_token = format!("refresh-{subject}");
        account
    }

    fn account(account_id: &str, remark: &str) -> StoredOfficialAccount {
        StoredOfficialAccount {
            id: String::new(),
            name: account_id.into(),
            remark: remark.into(),
            account_id: account_id.into(),
            email: format!("{account_id}@example.test"),
            credential: CodexAuthCredential {
                auth_mode: "chatgpt".into(),
                openai_api_key: None,
                tokens: CodexAuthTokens {
                    id_token: String::new(),
                    access_token: format!("{account_id}-access"),
                    refresh_token: String::new(),
                    account_id: account_id.into(),
                },
                last_refresh: "2026-07-31T00:00:00Z".into(),
            },
            source: OfficialAccountSource::ProxyImport,
            expires_at: None,
            quota: ProviderAccountQuota::default(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn estimate_windows_uses_explicit_duration_and_never_invents_missing_primary() {
        let quota = ProviderAccountQuota {
            status: QuotaStatus::Success,
            data: Some(QuotaData::Windowed {
                primary: None,
                secondary: Some(QuotaWindow {
                    used_percent: 20.0,
                    remaining_percent: 80.0,
                    window_seconds: Some(604_800),
                    reset_at: Some(100),
                }),
            }),
            ..Default::default()
        };
        assert_eq!(estimate_windows(&quota), vec![(604_800, 100, 20.0)]);
    }

    #[test]
    fn estimate_windows_falls_back_only_for_existing_legacy_slots() {
        let quota = ProviderAccountQuota {
            status: QuotaStatus::Success,
            data: Some(QuotaData::Windowed {
                primary: Some(QuotaWindow {
                    used_percent: 10.0,
                    remaining_percent: 90.0,
                    window_seconds: None,
                    reset_at: Some(20),
                }),
                secondary: Some(QuotaWindow {
                    used_percent: 20.0,
                    remaining_percent: 80.0,
                    window_seconds: None,
                    reset_at: Some(30),
                }),
            }),
            ..Default::default()
        };
        assert_eq!(
            estimate_windows(&quota),
            vec![(18_000, 20, 10.0), (604_800, 30, 20.0)]
        );
    }

    #[test]
    fn quota_estimation_retries_when_any_window_crosses_a_reset() {
        let results = vec![
            QuotaEstimateWindowResult {
                window_seconds: FIVE_HOURS_SECONDS,
                reset_at: 100,
                success: true,
                estimate: None,
                reason: None,
            },
            QuotaEstimateWindowResult {
                window_seconds: SEVEN_DAYS_SECONDS,
                reset_at: 200,
                success: true,
                estimate: None,
                reason: None,
            },
        ];
        assert!(quota_estimation_crossed_reset(&results, 100));
        assert!(!quota_estimation_crossed_reset(&results, 99));
    }

    #[test]
    fn each_refreshed_quota_window_clears_its_own_old_estimate_including_retry_window() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let account = store
            .save_official_account(&account("workspace", ""))
            .unwrap();
        let old = QuotaEstimate {
            window_seconds: FIVE_HOURS_SECONDS,
            reset_at: 100,
            estimated_total_microusd: 100,
            estimated_at: 1,
            calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
        };
        let first_current = QuotaEstimate {
            reset_at: 200,
            ..old.clone()
        };
        let retry_current = QuotaEstimate {
            reset_at: 300,
            ..old.clone()
        };
        store
            .save_official_account_quota_estimates(
                &account.id,
                &[old.clone(), first_current.clone(), retry_current.clone()],
            )
            .unwrap();

        let quota_for = |reset_at| ProviderAccountQuota {
            status: QuotaStatus::Success,
            data: Some(QuotaData::Windowed {
                primary: Some(QuotaWindow {
                    used_percent: 25.0,
                    remaining_percent: 75.0,
                    window_seconds: Some(FIVE_HOURS_SECONDS),
                    reset_at: Some(reset_at),
                }),
                secondary: None,
            }),
            ..Default::default()
        };
        clear_estimates_for_current_quota(&store, &account.id, &quota_for(200)).unwrap();
        let after_first = store.official_account(&account.id).unwrap().quota.estimates;
        assert!(after_first.contains(&old));
        assert!(!after_first.contains(&first_current));
        assert!(after_first.contains(&retry_current));

        clear_estimates_for_current_quota(&store, &account.id, &quota_for(300)).unwrap();
        let after_retry = store.official_account(&account.id).unwrap().quota.estimates;
        assert_eq!(after_retry, vec![old]);
    }

    #[test]
    fn successful_quota_refresh_clears_estimates_for_the_refreshed_windows() {
        let estimate = QuotaEstimate {
            window_seconds: FIVE_HOURS_SECONDS,
            reset_at: 100,
            estimated_total_microusd: 123,
            estimated_at: 1,
            calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
        };
        let mut snapshot = ProviderAccountQuota {
            estimates: vec![estimate.clone()],
            error: Some("旧错误".into()),
            ..Default::default()
        };
        apply_successful_quota_snapshot(
            &mut snapshot,
            QuotaData::Windowed {
                primary: Some(QuotaWindow {
                    used_percent: 20.0,
                    remaining_percent: 80.0,
                    window_seconds: Some(FIVE_HOURS_SECONDS),
                    reset_at: Some(100),
                }),
                secondary: None,
            },
            Some("plus".into()),
            ResetCreditSummary::default(),
            42,
        );
        assert_eq!(snapshot.status, QuotaStatus::Success);
        assert!(snapshot.estimates.is_empty());
        assert_eq!(snapshot.fetched_at, Some(42));
        assert!(snapshot.error.is_none());
    }

    #[test]
    fn completed_login_saves_the_account_without_switching_connections() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();

        let view = save_completed_login(&store, &account("new-account", "")).unwrap();

        assert!(!view.active);
        assert!(matches!(
            store.snapshot().unwrap().active.kind,
            ActiveKind::None
        ));
    }

    #[test]
    fn completed_login_adds_a_different_user_in_the_active_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let active = store
            .save_official_account(&oauth_account(
                "shared-workspace",
                "user-1",
                "first@example.test",
            ))
            .unwrap();
        store
            .connections_activate_official_account(&active.id)
            .unwrap();
        codex::connections_activate_official_account(&home, &active.credential, None).unwrap();
        let config_before = fs::read(home.join("config.toml")).unwrap();
        let auth_before = fs::read(home.join("auth.json")).unwrap();

        let added = save_completed_login(
            &store,
            &oauth_account("shared-workspace", "user-2", "second@example.test"),
        )
        .unwrap();

        assert_ne!(added.id, active.id);
        assert!(!added.active);
        let state = store.snapshot().unwrap();
        assert_eq!(state.official_accounts.len(), 2);
        assert_eq!(state.active.account_id.as_deref(), Some(active.id.as_str()));
        assert_eq!(fs::read(home.join("config.toml")).unwrap(), config_before);
        assert_eq!(fs::read(home.join("auth.json")).unwrap(), auth_before);
    }

    #[test]
    fn completed_login_updates_the_same_user_and_preserves_local_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let mut original = oauth_account("shared-workspace", "user-1", "first@example.test");
        original.remark = "保留的备注".into();
        original.quota.status = QuotaStatus::Success;
        original.quota.fetched_at = Some(42);
        let original = store.save_official_account(&original).unwrap();
        store
            .connections_activate_official_account(&original.id)
            .unwrap();
        codex::connections_activate_official_account(&home, &original.credential, None).unwrap();
        let auth_before = fs::read(home.join("auth.json")).unwrap();
        let mut refreshed = oauth_account("shared-workspace", "user-1", "renamed@example.test");
        refreshed.credential.tokens.refresh_token = "refresh-updated".into();

        let updated = save_completed_login(&store, &refreshed).unwrap();

        assert_eq!(updated.id, original.id);
        assert!(updated.active);
        assert_eq!(updated.remark, "保留的备注");
        assert_eq!(updated.quota.status, QuotaStatus::Success);
        assert_eq!(updated.quota.fetched_at, Some(42));
        assert_eq!(store.snapshot().unwrap().official_accounts.len(), 1);
        assert_eq!(
            store
                .official_account(&original.id)
                .unwrap()
                .credential
                .tokens
                .refresh_token,
            "refresh-updated"
        );
        assert_eq!(fs::read(home.join("auth.json")).unwrap(), auth_before);
    }

    #[tokio::test]
    async fn active_account_quota_refresh_does_not_rotate_or_write_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join("auth.json"), b"external-codex-auth").unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let mut expired = account("active-account", "");
        expired.expires_at = Some(1);
        let saved = store.save_official_account(&expired).unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let quota_account = account_for_quota(
            &store,
            &AuthCenter::default(),
            &ActivationLock::default(),
            &saved.id,
        )
        .await
        .unwrap();

        assert_eq!(quota_account.credential, saved.credential);
        assert_eq!(
            fs::read(home.join("auth.json")).unwrap(),
            b"external-codex-auth"
        );
    }

    #[test]
    fn batch_remark_command_returns_redacted_views_in_request_order() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let first = store
            .save_official_account(&account("first", "old-first"))
            .unwrap();
        let second = store
            .save_official_account(&account("second", "old-second"))
            .unwrap();
        store
            .connections_activate_official_account(&second.id)
            .unwrap();

        let views = connections_update_account_remarks_in_store(
            &store,
            vec![
                AccountRemarkUpdate {
                    id: second.id.clone(),
                    remark: "  new-second  ".into(),
                },
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "new-first".into(),
                },
            ],
        )
        .unwrap();

        assert_eq!(
            views
                .iter()
                .map(|view| (view.id.as_str(), view.remark.as_str(), view.active))
                .collect::<Vec<_>>(),
            vec![
                (second.id.as_str(), "new-second", true),
                (first.id.as_str(), "new-first", false),
            ]
        );
        assert!(!serde_json::to_string(&views).unwrap().contains("access"));
    }

    #[tokio::test]
    async fn batch_delete_command_uses_activation_lock_and_store_batch_delete() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let first = store.save_official_account(&account("first", "")).unwrap();
        let active = store.save_official_account(&account("active", "")).unwrap();
        store
            .connections_activate_official_account(&active.id)
            .unwrap();
        let activation = ActivationLock::default();

        connections_delete_accounts_in_store(
            &store,
            &activation,
            vec![first.id.clone(), first.id.clone()],
        )
        .await
        .unwrap();
        assert!(store.official_account(&first.id).is_err());
        assert!(
            connections_delete_accounts_in_store(&store, &activation, vec![active.id.clone()],)
                .await
                .is_err()
        );
        assert!(store.official_account(&active.id).is_ok());
    }

    #[tokio::test]
    async fn connections_refresh_all_quota_continues_after_expired_proxy_accounts() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        for account_id in ["first", "second"] {
            store
                .save_official_account(&StoredOfficialAccount {
                    id: String::new(),
                    name: account_id.into(),
                    remark: String::new(),
                    account_id: account_id.into(),
                    email: String::new(),
                    credential: CodexAuthCredential {
                        auth_mode: "chatgpt".into(),
                        openai_api_key: None,
                        tokens: CodexAuthTokens {
                            id_token: String::new(),
                            access_token: format!("{account_id}-access"),
                            refresh_token: String::new(),
                            account_id: account_id.into(),
                        },
                        last_refresh: "2026-07-31T00:00:00Z".into(),
                    },
                    source: OfficialAccountSource::ProxyImport,
                    expires_at: Some(1),
                    quota: ProviderAccountQuota::default(),
                    created_at: 0,
                    updated_at: 0,
                })
                .unwrap();
        }

        let results = connections_refresh_all_quota_in_store(
            &store,
            &AuthCenter::default(),
            &ApiClient::default(),
            &ActivationLock::default(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|result| result.quota.status == QuotaStatus::Unauthorized)
        );
        assert!(
            results
                .iter()
                .all(|result| result.quota.last_attempt_at.is_some())
        );
        let persisted = store
            .read(|state| {
                state
                    .official_accounts
                    .iter()
                    .map(|account| account.quota.status)
                    .collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(persisted, vec![QuotaStatus::Unauthorized; 2]);
    }

    #[tokio::test]
    async fn active_account_refresh_syncs_cookie_to_codex() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let saved = store
            .save_official_account(&StoredOfficialAccount {
                id: String::new(),
                name: "Cookie".into(),
                remark: "备用账号".into(),
                account_id: "cookie-account".into(),
                email: String::new(),
                credential: CodexAuthCredential {
                    auth_mode: "chatgpt".into(),
                    openai_api_key: None,
                    tokens: CodexAuthTokens {
                        id_token: String::new(),
                        access_token: "at-cookie-secret".into(),
                        refresh_token: String::new(),
                        account_id: "cookie-account".into(),
                    },
                    last_refresh: "2026-07-31T00:00:00Z".into(),
                },
                source: OfficialAccountSource::ProxyImport,
                expires_at: Some(chrono::Utc::now().timestamp().saturating_add(3600)),
                quota: ProviderAccountQuota::default(),
                created_at: 0,
                updated_at: 0,
            })
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        let refreshed = AuthCenter::default()
            .refresh_account(&store, &saved.id)
            .await
            .unwrap();
        assert_eq!(refreshed.id, saved.id);

        let view = crate::credential_maintenance::maintain_login(
            &store,
            &AuthCenter::default(),
            &manager,
            &ActivationLock::default(),
            &proxy,
            &saved.id,
        )
        .await
        .unwrap();

        assert!(view.account.active);
        assert_eq!(view.account.remark, "备用账号");
        let auth: serde_json::Value =
            serde_json::from_slice(&fs::read(home.join("auth.json")).unwrap()).unwrap();
        assert_eq!(auth["personal_access_token"], "at-cookie-secret");
    }

    #[tokio::test]
    async fn successful_rotation_is_durable_before_the_next_exchange() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let rows = proxy_import::parse_proxy_credentials(r#"[{"refresh_token":"rt-one"},{"refresh_token":"rt-two"},{"refresh_token":"rt-three"}]"#).unwrap();
        let mut calls = 0;
        let (saved, partial) = import_credential_rows(&store, rows, None, |_, _| {
            calls += 1;
            let result = if calls == 1 {
                Ok(oauth_account("workspace", "first", "first@example.test"))
            } else {
                let reopened = Store::open(store.root().to_owned()).unwrap();
                assert_eq!(
                    reopened.snapshot().unwrap().official_accounts[0]
                        .credential
                        .tokens
                        .refresh_token,
                    "refresh-first"
                );
                Err(ImportExchangeError::Rejected(AppError::Internal(
                    "模拟换发失败".into(),
                )))
            };
            std::future::ready(result)
        })
        .await
        .unwrap();
        let partial = partial.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(calls, 2);
        assert_eq!(partial.retry_from, 1);
        assert!(partial.message.contains("第 2 项导入失败，前 1 项已保存"));
        assert!(!partial.message.contains("rt-"));
    }

    #[tokio::test]
    async fn rotated_import_survives_failure_before_the_state_journal() {
        for on_disk in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("fixture-data");
            let store = Store::open(root.clone()).unwrap();
            let journal = root.join("state-transaction.json");
            let recovery = root.join("pending-official-import.json");
            let rows = proxy_import::parse_proxy_credentials(
                r#"[{"refresh_token":"rt-first"},{"refresh_token":"rt-second"},{"refresh_token":"rt-third"}]"#,
            )
            .unwrap();
            let mut exchanges = 0;
            let (saved, partial) =
                import_credential_rows(&store, rows, Some(("Team", 0)), |_, name| {
                    exchanges += 1;
                    let mut account = match exchanges {
                        1 => oauth_account("workspace", "first", "first@example.test"),
                        2 => {
                            // The rotation succeeded before any main journal could be written.
                            fs::create_dir(&journal).unwrap();
                            if !on_disk {
                                fs::create_dir(&recovery).unwrap();
                            }
                            oauth_account("workspace", "second", "second@example.test")
                        }
                        _ => panic!("the next token must wait while a local save is pending"),
                    };
                    account.name = name.unwrap();
                    std::future::ready(Ok(account))
                })
                .await
                .unwrap();
            assert_eq!(exchanges, 2);
            assert_eq!(saved.len(), 1);
            let partial = partial.unwrap();
            assert_eq!(partial.retry_from, 2);
            assert!(partial.message.contains("暂存"));
            assert!(!partial.message.contains("refresh-second"));
            assert_eq!(recovery.is_file(), on_disk);
            fs::remove_dir(journal).unwrap();

            let recovered = if on_disk {
                drop(store);
                Store::open(root.clone()).unwrap()
            } else {
                fs::remove_dir(&recovery).unwrap();
                store.retry_pending_official_import().unwrap();
                store
            };
            let accounts = recovered.snapshot().unwrap().official_accounts;
            assert_eq!(accounts.len(), 2);
            assert_eq!(accounts[0].credential.tokens.refresh_token, "refresh-first");
            assert_eq!(
                accounts[1].credential.tokens.refresh_token,
                "refresh-second"
            );
            assert_eq!(accounts[1].name, "Team #2");
            assert!(!recovery.exists());

            let remaining = proxy_import::parse_proxy_credentials("rt-third").unwrap();
            let (saved, partial) =
                import_credential_rows(&recovered, remaining, Some(("Team", 2)), |row, name| {
                    assert_eq!(row.refresh_token.as_deref(), Some("rt-third"));
                    assert_eq!(name.as_deref(), Some("Team #3"));
                    std::future::ready(Ok(oauth_account(
                        "workspace",
                        "third",
                        "third@example.test",
                    )))
                })
                .await
                .unwrap();
            assert_eq!(saved.len(), 1);
            assert!(partial.is_none());
            assert_eq!(
                Store::open(root)
                    .unwrap()
                    .snapshot()
                    .unwrap()
                    .official_accounts
                    .len(),
                3
            );
        }
    }

    #[tokio::test]
    async fn rotated_import_persists_a_valid_email_longer_than_the_display_name_limit() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("fixture-data");
        let store = Store::open(root.clone()).unwrap();
        let email = format!("{}@{}.example.test", "a".repeat(64), "b".repeat(40));
        let minted = oauth_account("workspace", "new", &email).credential.tokens;
        let rows = proxy_import::parse_proxy_credentials("rt-old").unwrap();
        let (saved, partial) = import_credential_rows(&store, rows, None, |row, name| {
            let response = axum::http::Response::builder()
                .status(200)
                .body(
                    serde_json::json!({
                        "access_token": minted.access_token,
                        "id_token": minted.id_token,
                        "refresh_token": minted.refresh_token,
                    })
                    .to_string(),
                )
                .unwrap();
            AuthCenter::account_from_exchange_response(response.into(), row, name)
        })
        .await
        .unwrap();
        assert!(partial.is_none());
        assert_eq!(saved.len(), 1);
        drop(store);
        let stored = Store::open(root)
            .unwrap()
            .official_account(&saved[0].id)
            .unwrap();
        assert_eq!(stored.email, email);
        assert_eq!(stored.name, "Cookie 账号");
        assert_eq!(stored.credential.tokens.refresh_token, minted.refresh_token);
    }

    #[tokio::test]
    async fn retry_draft_retains_rejected_credentials_and_drops_unconfirmed_rotations() {
        let temp = tempfile::tempdir().unwrap();
        for (status, retry_from) in [(400, 0), (408, 1), (503, 1), (200, 1)] {
            let store = Store::open(temp.path().join(status.to_string())).unwrap();
            let rows = proxy_import::parse_proxy_credentials(
                r#"[{"refresh_token":"rt-first"},{"refresh_token":"rt-next"}]"#,
            )
            .unwrap();
            let (saved, partial) =
                import_credential_rows(&store, rows, None, |row, name| async move {
                    let response = axum::http::Response::builder()
                        .status(status)
                        .body("incomplete reply")
                        .unwrap();
                    AuthCenter::account_from_exchange_response(response.into(), row, name).await
                })
                .await
                .unwrap();
            assert!(saved.is_empty());
            let partial = partial.unwrap();
            assert_eq!(partial.retry_from, retry_from);
            assert_eq!(partial.message.contains("重新授权"), retry_from == 1);
        }
    }

    #[tokio::test]
    async fn import_and_refresh_share_the_restart_guard_for_the_same_token() {
        let temp = tempfile::tempdir().unwrap();
        for first_is_import in [false, true] {
            let root = temp
                .path()
                .join(format!("first-is-import-{first_is_import}"));
            let store = Store::open(root.clone()).unwrap();
            let mut existing = oauth_account("workspace", "fixture", "fixture@example.test");
            existing.credential.tokens.refresh_token = "rt-guard-fixture".into();
            let existing = store.save_official_account(&existing).unwrap();
            let rows = || proxy_import::parse_proxy_credentials("rt-guard-fixture").unwrap();
            if first_is_import {
                let (_, partial) = import_credential_rows(&store, rows(), None, |_, _| async {
                    Err(ImportExchangeError::MayHaveRotated)
                })
                .await
                .unwrap();
                assert_eq!(partial.unwrap().retry_from, 1);
            } else {
                store.record_official_refresh_attempt(&existing).unwrap();
            }
            drop(store);

            let reopened = Store::open(root).unwrap();
            assert!(matches!(
                reopened.record_official_refresh_attempt(&existing),
                Err(AppError::InvalidConfig(_))
            ));
            let (saved, partial) = import_credential_rows(&reopened, rows(), None, |_, _| async {
                panic!("the local guard must stop a second request");
            })
            .await
            .unwrap();
            assert!(saved.is_empty());
            assert_eq!(partial.unwrap().retry_from, 1);
        }
    }

    #[tokio::test]
    async fn retry_keeps_batch_names_and_excludes_a_rotated_token_if_saving_fails() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_owned()).unwrap();
        let rows = proxy_import::parse_proxy_credentials("rt-fixture").unwrap();
        let (saved, partial) =
            import_credential_rows(&store, rows, Some(("Team", 2)), |_, name| async move {
                assert_eq!(name.as_deref(), Some("Team #3"));
                let mut account = oauth_account("team", "third", "test@example.test");
                account.credential.tokens.access_token.clear();
                Ok(account)
            })
            .await
            .unwrap();
        assert!(saved.is_empty());
        let partial = partial.unwrap();
        assert_eq!(partial.retry_from, 1);
        assert!(partial.message.contains("重新授权"));
        assert!(store.snapshot().unwrap().official_accounts.is_empty());
    }

    #[tokio::test]
    async fn repeated_rotating_token_or_invalid_local_account_aborts_before_exchange() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_owned()).unwrap();
        for second in [
            serde_json::json!({"refresh_token":"rt-fixture"}),
            serde_json::json!({"access_token":"at-fixture","email":"x".repeat(321)}),
        ] {
            let input = serde_json::json!([{"refresh_token":"rt-fixture"}, second]).to_string();
            let rows = proxy_import::parse_proxy_credentials(&input).unwrap();
            assert!(
                import_credential_rows(&store, rows, None, |_, _| async {
                    panic!("invalid batch must never exchange a token")
                })
                .await
                .is_err()
            );
        }
        assert!(store.snapshot().unwrap().official_accounts.is_empty());
    }

    #[tokio::test]
    async fn cookie_import_saves_a_new_account_without_changing_active_codex_files() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let active = store
            .save_official_account(&oauth_account(
                "shared-workspace",
                "active-user",
                "active@example.test",
            ))
            .unwrap();
        store
            .connections_activate_official_account(&active.id)
            .unwrap();
        codex::connections_activate_official_account(&home, &active.credential, None).unwrap();
        let config_before = fs::read(home.join("config.toml")).unwrap();
        let auth_before = fs::read(home.join("auth.json")).unwrap();

        let result = connections_import_cookie_in_store(
            &store,
            &AuthCenter::default(),
            &ActivationLock::default(),
            Some(("新账号", 0)),
            Some("shared-workspace".into()),
            "at-new-account".into(),
        )
        .await
        .unwrap();

        assert_eq!(result.accounts.len(), 1);
        assert!(!result.accounts[0].active);
        assert_eq!(result.accounts[0].account_id, "shared-workspace");
        assert_eq!(
            store.snapshot().unwrap().active.account_id.as_deref(),
            Some(active.id.as_str())
        );
        assert_eq!(fs::read(home.join("config.toml")).unwrap(), config_before);
        assert_eq!(fs::read(home.join("auth.json")).unwrap(), auth_before);
    }

    #[tokio::test]
    async fn active_cookie_reimport_only_updates_saved_credentials_and_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let mut saved_account = account("cookie-account", "保留的备注");
        saved_account.email.clear();
        let saved = store.save_official_account(&saved_account).unwrap();
        let quota = ProviderAccountQuota {
            status: QuotaStatus::Success,
            fetched_at: Some(42),
            ..Default::default()
        };
        store.save_official_account_quota(&saved.id, quota).unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();
        codex::connections_activate_official_account(&home, &saved.credential, None).unwrap();
        let config_before = fs::read(home.join("config.toml")).unwrap();
        let auth_before = fs::read(home.join("auth.json")).unwrap();

        let result = connections_import_cookie_in_store(
            &store,
            &AuthCenter::default(),
            &ActivationLock::default(),
            Some(("更新后的名称", 0)),
            Some("cookie-account".into()),
            "at-cookie-reimported".into(),
        )
        .await
        .unwrap();

        let view = &result.accounts[0];
        assert!(view.active);
        assert_eq!(view.id, saved.id);
        assert_eq!(view.remark, "保留的备注");
        assert_eq!(view.quota.status, QuotaStatus::Success);
        assert_eq!(view.quota.fetched_at, Some(42));
        assert_eq!(
            store
                .official_account(&saved.id)
                .unwrap()
                .credential
                .tokens
                .access_token,
            "at-cookie-reimported"
        );
        assert_eq!(fs::read(home.join("config.toml")).unwrap(), config_before);
        assert_eq!(fs::read(home.join("auth.json")).unwrap(), auth_before);
    }
}
