use crate::{
    activation::{sync_active_codex_configuration, sync_active_openai_credential},
    auth_center::AuthCenter,
    chat_proxy::ChatProxyRegistry,
    codex::{self, ConfigManager},
    models::{
        AppError, CredentialMaintenanceOutcome, CredentialMaintenanceResult,
        CredentialRefreshState, CredentialRefreshStatus, LoginVerificationStatus,
        ProviderAccountQuota, QuotaStatus, StoredOfficialAccount,
    },
    state::ActivationLock,
    storage::Store,
};

pub(crate) const REFRESH_WINDOW_SECS: i64 = 2 * 24 * 60 * 60;
pub(crate) const UNKNOWN_EXPIRY_RETRY_SECS: i64 = 24 * 60 * 60;
pub(crate) const RETRY_DELAYS_SECS: [i64; 4] = [60, 5 * 60, 15 * 60, 60 * 60];

fn codex_is_running(configured: Option<&str>) -> bool {
    #[cfg(test)]
    {
        let _ = configured;
        false
    }
    #[cfg(not(test))]
    {
        crate::platform::codex_app_running(configured)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceDecision {
    NotRefreshable,
    ReauthenticationRequired,
    WaitingRetry,
    Unchanged,
    Refresh,
}

pub(crate) fn needs_credential_refresh(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_some_and(|expires_at| expires_at <= now.saturating_add(REFRESH_WINDOW_SECS))
}

pub(crate) fn maintenance_decision(
    account: &StoredOfficialAccount,
    state: &CredentialRefreshState,
    now: i64,
    force: bool,
) -> MaintenanceDecision {
    if account.credential.tokens.refresh_token.trim().is_empty() {
        return MaintenanceDecision::NotRefreshable;
    }
    if state.status == CredentialRefreshStatus::ReauthenticationRequired {
        return MaintenanceDecision::ReauthenticationRequired;
    }
    if state.next_retry_at.is_some_and(|next| next > now) {
        return MaintenanceDecision::WaitingRetry;
    }
    if force {
        return MaintenanceDecision::Refresh;
    }
    if needs_credential_refresh(account.expires_at, now) {
        return MaintenanceDecision::Refresh;
    }
    if account.expires_at.is_none()
        && state
            .last_attempt_at
            .is_none_or(|last| now.saturating_sub(last) >= UNKNOWN_EXPIRY_RETRY_SECS)
    {
        return MaintenanceDecision::Refresh;
    }
    MaintenanceDecision::Unchanged
}

fn save_refresh_state_if_changed(
    store: &Store,
    id: &str,
    previous: &CredentialRefreshState,
    next: CredentialRefreshState,
) -> Result<(), AppError> {
    if previous != &next {
        store.save_credential_refresh_state(id, next)?;
    }
    Ok(())
}

fn refreshed_state(previous: &CredentialRefreshState, now: i64) -> CredentialRefreshState {
    CredentialRefreshState {
        status: CredentialRefreshStatus::Healthy,
        last_attempt_at: Some(now),
        last_success_at: Some(now),
        next_retry_at: None,
        retry_count: 0,
        last_refresh_at: Some(now),
        last_sync_at: previous.last_sync_at,
        last_check_at: previous.last_check_at,
        verification: previous.verification,
    }
}

fn retry_state(previous: &CredentialRefreshState, now: i64) -> CredentialRefreshState {
    let retry_count = previous.retry_count.saturating_add(1);
    let index = usize::from(retry_count.saturating_sub(1)).min(RETRY_DELAYS_SECS.len() - 1);
    CredentialRefreshState {
        status: CredentialRefreshStatus::WaitingRetry,
        last_attempt_at: Some(now),
        last_success_at: previous.last_success_at,
        next_retry_at: Some(now.saturating_add(RETRY_DELAYS_SECS[index])),
        retry_count,
        last_refresh_at: previous.last_refresh_at,
        last_sync_at: previous.last_sync_at,
        last_check_at: previous.last_check_at,
        verification: previous.verification,
    }
}

fn reauthentication_state(previous: &CredentialRefreshState, now: i64) -> CredentialRefreshState {
    CredentialRefreshState {
        status: CredentialRefreshStatus::ReauthenticationRequired,
        last_attempt_at: Some(now),
        last_success_at: previous.last_success_at,
        next_retry_at: None,
        retry_count: previous.retry_count,
        last_refresh_at: previous.last_refresh_at,
        last_sync_at: previous.last_sync_at,
        last_check_at: previous.last_check_at,
        verification: previous.verification,
    }
}

fn not_refreshable_state(previous: &CredentialRefreshState) -> CredentialRefreshState {
    CredentialRefreshState {
        status: CredentialRefreshStatus::NotRefreshable,
        last_attempt_at: previous.last_attempt_at,
        last_success_at: previous.last_success_at,
        next_retry_at: None,
        retry_count: 0,
        last_refresh_at: previous.last_refresh_at,
        last_sync_at: previous.last_sync_at,
        last_check_at: previous.last_check_at,
        verification: previous.verification,
    }
}

fn managed_by_codex_state(
    previous: &CredentialRefreshState,
    now: i64,
    synced: bool,
) -> CredentialRefreshState {
    CredentialRefreshState {
        status: CredentialRefreshStatus::ManagedByCodex,
        last_attempt_at: previous.last_attempt_at,
        last_success_at: previous.last_success_at,
        next_retry_at: None,
        retry_count: 0,
        last_refresh_at: previous.last_refresh_at,
        last_sync_at: synced.then_some(now).or(previous.last_sync_at),
        last_check_at: previous.last_check_at,
        verification: previous.verification,
    }
}

/// 统一后台和“检查/更新登录”入口。它只在 Codex 已停止时尝试 OAuth；
/// 运行中仅读取 auth.json，并让 Codex 独占轮换 Refresh Token。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn maintain_account(
    store: &Store,
    center: &AuthCenter,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    id: &str,
) -> Result<CredentialMaintenanceResult, AppError> {
    maintain_account_with_running(
        store,
        center,
        manager,
        activation,
        proxy,
        id,
        false,
        codex_is_running,
    )
    .await
}

/// 手动入口仅绕过到期阈值；已经生效的重试等待和重新登录状态仍然优先。
pub(crate) async fn maintain_login(
    store: &Store,
    center: &AuthCenter,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    id: &str,
) -> Result<CredentialMaintenanceResult, AppError> {
    maintain_account_with_running(
        store,
        center,
        manager,
        activation,
        proxy,
        id,
        true,
        codex_is_running,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn maintain_account_with_running(
    store: &Store,
    center: &AuthCenter,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    id: &str,
    force: bool,
    running: impl FnOnce(Option<&str>) -> bool,
) -> Result<CredentialMaintenanceResult, AppError> {
    let _guard = activation.0.lock().await;
    let now = chrono::Utc::now().timestamp();
    let (account, mut state, active) = store.official_account_for_maintenance(id)?;
    let home = codex::home(&store.codex_home_setting()?);
    let configured = store.codex_app_setting()?;
    if active && running(configured.as_deref()) {
        let synced = sync_active_openai_credential(store, &home)?;
        let next = managed_by_codex_state(&state, now, synced);
        if synced {
            // 严格更新凭据会清除旧维护状态；即使显示状态未变也必须重新写入。
            store.save_credential_refresh_state(&account.id, next)?;
        } else {
            save_refresh_state_if_changed(store, &account.id, &state, next)?;
        }
        return Ok(CredentialMaintenanceResult {
            account: store.official_account_view(&account.id)?,
            outcome: if synced {
                CredentialMaintenanceOutcome::SyncedFromCodex
            } else {
                CredentialMaintenanceOutcome::ManagedByCodex
            },
        });
    }

    // 停止后先接收 Codex 最后的 auth.json，再决定是否需要轮换凭据。
    let synced = active && sync_active_openai_credential(store, &home)?;
    if synced {
        // 从 Codex 的 auth.json 接收了较新的凭据时，只更新时间戳；
        // 刷新成功与在线验证的结论仍由各自路径维护。
        let next = CredentialRefreshState {
            last_sync_at: Some(now),
            ..state
        };
        store.save_credential_refresh_state(&account.id, next.clone())?;
        state = next;
    }
    let account = store.official_account(&account.id)?;
    let decision = maintenance_decision(&account, &state, now, force);
    match decision {
        MaintenanceDecision::NotRefreshable => {
            save_refresh_state_if_changed(
                store,
                &account.id,
                &state,
                not_refreshable_state(&state),
            )?;
        }
        MaintenanceDecision::ReauthenticationRequired
        | MaintenanceDecision::WaitingRetry
        | MaintenanceDecision::Unchanged => {}
        MaintenanceDecision::Refresh => {
            let attempted = CredentialRefreshState {
                last_attempt_at: Some(now),
                ..state.clone()
            };
            store.save_credential_refresh_state(&account.id, attempted)?;
            match if force {
                center.refresh_login(store, &account.id).await
            } else {
                center
                    .refresh_account_for_maintenance(store, &account.id)
                    .await
            } {
                Ok(result) if result.refreshed => {
                    store
                        .save_credential_refresh_state(&account.id, refreshed_state(&state, now))?;
                    if active {
                        sync_active_codex_configuration(store, manager, proxy).await?;
                    }
                    return Ok(CredentialMaintenanceResult {
                        account: store.official_account_view(&account.id)?,
                        outcome: CredentialMaintenanceOutcome::Refreshed,
                    });
                }
                Ok(_) => {}
                Err(AppError::InvalidConfig(_)) => {
                    store.save_credential_refresh_state(
                        &account.id,
                        reauthentication_state(&state, now),
                    )?;
                    return Ok(CredentialMaintenanceResult {
                        account: store.official_account_view(&account.id)?,
                        outcome: CredentialMaintenanceOutcome::ReauthenticationRequired,
                    });
                }
                Err(error) => {
                    store.save_credential_refresh_state(&account.id, retry_state(&state, now))?;
                    tracing::warn!(
                        operation = "credential_maintenance_refresh",
                        account_id = account.id.as_str(),
                        error = %error,
                        "credential refresh failed; retry scheduled"
                    );
                    return Ok(CredentialMaintenanceResult {
                        account: store.official_account_view(&account.id)?,
                        outcome: CredentialMaintenanceOutcome::WaitingRetry,
                    });
                }
            }
        }
    }
    // Codex 已停止时，未到刷新阈值的活动账号也要把已同步的当前凭据写回
    // auth.json，保证下一次启动读取的是同一个持久快照。
    if active {
        sync_active_codex_configuration(store, manager, proxy).await?;
    }
    Ok(CredentialMaintenanceResult {
        account: store.official_account_view(&account.id)?,
        outcome: if synced {
            CredentialMaintenanceOutcome::SyncedFromCodex
        } else if decision == MaintenanceDecision::WaitingRetry {
            CredentialMaintenanceOutcome::WaitingRetry
        } else if decision == MaintenanceDecision::ReauthenticationRequired {
            CredentialMaintenanceOutcome::ReauthenticationRequired
        } else if decision == MaintenanceDecision::NotRefreshable {
            CredentialMaintenanceOutcome::NotRefreshable
        } else {
            CredentialMaintenanceOutcome::Unchanged
        },
    })
}

/// 只记录脱敏在线检查结论；它不改变刷新重试和成功时间。
pub(crate) fn record_login_verification(
    store: &Store,
    id: &str,
    quota: &ProviderAccountQuota,
) -> Result<(), AppError> {
    let now = chrono::Utc::now().timestamp();
    let (_, previous, _) = store.official_account_for_maintenance(id)?;
    let verification = match quota.status {
        QuotaStatus::Success => LoginVerificationStatus::Valid,
        QuotaStatus::Unauthorized
            if quota
                .error
                .as_deref()
                .is_some_and(|error| error.contains("HTTP 402") || error.contains("HTTP 403")) =>
        {
            LoginVerificationStatus::WorkspaceOrPermission
        }
        QuotaStatus::Unauthorized => LoginVerificationStatus::Invalid,
        QuotaStatus::RateLimited
        | QuotaStatus::Error
        | QuotaStatus::Unsupported
        | QuotaStatus::Never => LoginVerificationStatus::CheckFailed,
    };
    store.save_credential_refresh_state(
        id,
        CredentialRefreshState {
            last_check_at: Some(now),
            verification,
            ..previous
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_once(
    store: &Store,
    center: &AuthCenter,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
) {
    let ids = match store.official_accounts_for_maintenance() {
        Ok(accounts) => accounts
            .into_iter()
            .map(|(account, _, _)| account.id)
            .collect::<Vec<_>>(),
        Err(_) => return,
    };
    for id in ids {
        // 单账号失败不阻断其他账号，状态已记录为重试或重新登录；错误本身不含响应体。
        let _ = maintain_account(store, center, manager, activation, proxy, &id).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        CodexAuthCredential, CodexAuthTokens, OfficialAccountSource, ProviderAccountQuota,
    };

    fn account(expires_at: Option<i64>) -> StoredOfficialAccount {
        StoredOfficialAccount {
            id: "account".into(),
            name: "账号".into(),
            remark: String::new(),
            account_id: "workspace".into(),
            email: "account@example.test".into(),
            credential: CodexAuthCredential {
                auth_mode: "chatgpt".into(),
                openai_api_key: None,
                tokens: CodexAuthTokens {
                    id_token: "test-id".into(),
                    access_token: "test-access".into(),
                    refresh_token: "test-refresh".into(),
                    account_id: "workspace".into(),
                },
                last_refresh: "2026-01-01T00:00:00Z".into(),
            },
            source: OfficialAccountSource::OpenAiOauth,
            expires_at,
            quota: ProviderAccountQuota::default(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn two_day_threshold_uses_an_inclusive_boundary() {
        let now = 1_800_000_000;
        assert!(!needs_credential_refresh(
            Some(now + REFRESH_WINDOW_SECS + 1),
            now
        ));
        assert!(needs_credential_refresh(
            Some(now + REFRESH_WINDOW_SECS),
            now
        ));
        assert!(needs_credential_refresh(
            Some(now + REFRESH_WINDOW_SECS - 1),
            now
        ));
        assert!(!needs_credential_refresh(Some(now + 5 * 24 * 60 * 60), now));
    }

    #[test]
    fn unknown_expiry_is_checked_at_most_once_per_day() {
        let now = 1_800_000_000;
        let account = account(None);
        let mut state = CredentialRefreshState {
            last_attempt_at: Some(now - UNKNOWN_EXPIRY_RETRY_SECS + 1),
            ..CredentialRefreshState::default()
        };
        assert_eq!(
            maintenance_decision(&account, &state, now, false),
            MaintenanceDecision::Unchanged
        );
        state.last_attempt_at = Some(now - UNKNOWN_EXPIRY_RETRY_SECS);
        assert_eq!(
            maintenance_decision(&account, &state, now, false),
            MaintenanceDecision::Refresh
        );
    }

    #[test]
    fn reauthentication_requirement_is_never_reclassified_as_healthy() {
        let now = 1_800_000_000;
        let account = account(Some(now));
        let state = CredentialRefreshState {
            status: CredentialRefreshStatus::ReauthenticationRequired,
            ..CredentialRefreshState::default()
        };
        assert_eq!(
            maintenance_decision(&account, &state, now, false),
            MaintenanceDecision::ReauthenticationRequired
        );
    }

    #[test]
    fn manual_refresh_bypasses_expiry_but_keeps_retry_and_reauthentication_gates() {
        let now = 1_800_000_000;
        let account = account(Some(now + 7 * 24 * 60 * 60));
        let mut state = CredentialRefreshState::default();

        assert_eq!(
            maintenance_decision(&account, &state, now, true),
            MaintenanceDecision::Refresh
        );

        state.next_retry_at = Some(now + 60);
        assert_eq!(
            maintenance_decision(&account, &state, now, true),
            MaintenanceDecision::WaitingRetry
        );

        state.next_retry_at = None;
        state.status = CredentialRefreshStatus::ReauthenticationRequired;
        assert_eq!(
            maintenance_decision(&account, &state, now, true),
            MaintenanceDecision::ReauthenticationRequired
        );
    }

    #[test]
    fn online_verification_records_timestamp_without_claiming_refresh_success() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let saved = store.save_official_account(&account(None)).unwrap();

        for (quota, expected) in [
            (
                ProviderAccountQuota {
                    status: QuotaStatus::Unauthorized,
                    error: Some("HTTP 401".into()),
                    ..ProviderAccountQuota::default()
                },
                LoginVerificationStatus::Invalid,
            ),
            (
                ProviderAccountQuota {
                    status: QuotaStatus::Unauthorized,
                    error: Some("HTTP 402".into()),
                    ..ProviderAccountQuota::default()
                },
                LoginVerificationStatus::WorkspaceOrPermission,
            ),
            (
                ProviderAccountQuota {
                    status: QuotaStatus::Unauthorized,
                    error: Some("HTTP 403".into()),
                    ..ProviderAccountQuota::default()
                },
                LoginVerificationStatus::WorkspaceOrPermission,
            ),
            (
                ProviderAccountQuota {
                    status: QuotaStatus::RateLimited,
                    ..ProviderAccountQuota::default()
                },
                LoginVerificationStatus::CheckFailed,
            ),
        ] {
            record_login_verification(&store, &saved.id, &quota).unwrap();
            let state = store
                .official_account_view(&saved.id)
                .unwrap()
                .credential_refresh;
            assert_eq!(state.verification, expected);
            assert!(state.last_check_at.is_some());
            assert!(state.last_refresh_at.is_none());
        }
    }

    #[tokio::test]
    async fn reauthentication_requirement_persists_until_credentials_are_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let saved = store
            .save_official_account(&account(Some(chrono::Utc::now().timestamp())))
            .unwrap();
        let state = CredentialRefreshState {
            status: CredentialRefreshStatus::ReauthenticationRequired,
            last_attempt_at: Some(1),
            ..CredentialRefreshState::default()
        };
        store
            .save_credential_refresh_state(&saved.id, state.clone())
            .unwrap();

        let result = maintain_account_with_running(
            &store,
            &AuthCenter::default(),
            &ConfigManager::default(),
            &ActivationLock::default(),
            &ChatProxyRegistry::default(),
            &saved.id,
            false,
            |_| false,
        )
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            CredentialMaintenanceOutcome::ReauthenticationRequired
        );
        assert_eq!(result.account.credential_refresh, state);
    }

    #[tokio::test]
    async fn running_codex_manages_an_expiring_active_account_without_oauth_or_auth_write() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let saved = store
            .save_official_account(&account(Some(chrono::Utc::now().timestamp())))
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let result = maintain_account_with_running(
            &store,
            &AuthCenter::default(),
            &ConfigManager::default(),
            &ActivationLock::default(),
            &ChatProxyRegistry::default(),
            &saved.id,
            false,
            |_| true,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, CredentialMaintenanceOutcome::ManagedByCodex);
        assert_eq!(
            result.account.credential_refresh.status,
            CredentialRefreshStatus::ManagedByCodex
        );
        assert!(!home.join("auth.json").exists());
    }
}
