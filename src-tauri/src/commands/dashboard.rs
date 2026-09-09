use crate::{
    codex, commands::usage::local_today_range, local_usage::UsageLedger, model_unlock, models::*,
    network, platform, session_index::SessionIndex, state::ActivationLock, storage::Store,
};
use std::{fs, path::Path};
use tauri::State;

#[derive(Debug, Default)]
struct ActiveProjection {
    home: String,
    provider_count: usize,
    active_provider: Option<String>,
    active_kind: ActiveKind,
    active_account_id: Option<String>,
    active_account: Option<String>,
    active_quota: Option<ProviderAccountQuota>,
}

fn project_active(state: &AppConfig) -> ActiveProjection {
    let active_stored_provider = state
        .active
        .provider_id
        .as_deref()
        .and_then(|id| state.providers.iter().find(|provider| provider.id == id));
    let active_official_account = state
        .active
        .account_id
        .as_deref()
        .filter(|_| matches!(state.active.kind, ActiveKind::Official))
        .and_then(|id| {
            state
                .official_accounts
                .iter()
                .find(|account| account.id == id)
        });
    let active_provider = active_stored_provider
        .map(|provider| provider.name.clone())
        .or_else(|| {
            active_official_account.map(|account| format!("OpenAI · {}", account.display_name()))
        })
        .or_else(|| {
            matches!(state.active.kind, ActiveKind::Official).then(|| "OpenAI 账号".into())
        });
    ActiveProjection {
        home: state.codex.home.clone(),
        provider_count: state.providers.len(),
        active_provider,
        active_kind: state.active.kind,
        active_account_id: active_official_account.map(|account| account.id.clone()),
        active_account: active_official_account.map(|account| account.display_name().to_owned()),
        active_quota: active_official_account.map(|account| account.quota.clone()),
    }
}

fn configured_model(home: &Path) -> Option<String> {
    fs::read_to_string(home.join("config.toml"))
        .ok()
        .and_then(|text| text.parse::<toml_edit::DocumentMut>().ok())
        .and_then(|document| {
            document
                .get("model")
                .and_then(toml_edit::Item::as_str)
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(str::to_owned)
        })
}

#[tauri::command]
pub(crate) async fn dashboard_get(
    store: State<'_, Store>,
    index: State<'_, SessionIndex>,
    ledger: State<'_, UsageLedger>,
) -> Result<Dashboard, AppError> {
    let projection = store.read(project_active)?;
    let home = codex::home(&projection.home);
    let active_model = matches!(projection.active_kind, ActiveKind::Provider)
        .then(|| configured_model(&home))
        .flatten();
    let index = index.inner().clone();
    let scan_home = home.clone();
    let session_task =
        tokio::task::spawn_blocking(move || match index.load_with_database_count(&scan_home) {
            Ok((sessions, database_count)) => (sessions.len(), database_count, "可以读取".into()),
            Err(_) => (0, 0, "读取失败".into()),
        });
    let today_query = UsageQuery {
        range: local_today_range(),
        group_by: UsageGroupBy::Account,
    };
    let ledger = ledger.inner().clone();
    let usage_task = tokio::task::spawn_blocking(move || ledger.query(today_query));
    let (session_result, today_result) = tokio::try_join!(session_task, usage_task)
        .map_err(|error| AppError::Internal(error.to_string()))?;
    let (session_count, database_count, database_health) = session_result;
    let today = today_result?;
    Ok(Dashboard {
        provider_count: projection.provider_count,
        active_provider: projection.active_provider,
        active_kind: projection.active_kind,
        active_account_id: projection.active_account_id,
        active_account: projection.active_account,
        active_model,
        active_quota: projection.active_quota,
        codex_home: home.display().to_string(),
        database_count,
        session_count,
        database_health,
        today_usage: today.totals.tokens,
        today_requests: today.totals.requests,
        today_estimated_cost_microusd: today.totals.estimated_cost_microusd,
        today_subscription_tokens: today.totals.subscription_tokens,
        today_unpriced_tokens: today.totals.unpriced_tokens,
        today_partial_tokens: today.totals.partial_tokens,
        today_unattributed_tokens: today.totals.unattributed_tokens,
    })
}

fn settings_overview(store: &Store) -> Result<SettingsOverview, AppError> {
    let (home_setting, active) =
        store.read(|state| (state.codex.home.clone(), state.active.clone()))?;
    let home = codex::home(&home_setting);
    let inspection = codex::inspect(&home);
    let diagnostics = serde_json::json!({
        "dataDirectory": store.root(),
        "configFile": store.path(),
        "codex": &inspection,
        "active": active,
    });
    Ok(SettingsOverview {
        inspection,
        diagnostics,
        can_preview_custom: matches!(active.kind, ActiveKind::Provider),
    })
}

#[tauri::command]
pub(crate) fn settings_get_overview(store: State<Store>) -> Result<SettingsOverview, AppError> {
    settings_overview(&store)
}

#[tauri::command]
pub(crate) async fn settings_get_diagnostics(
    store: State<'_, Store>,
    index: State<'_, SessionIndex>,
) -> Result<SupportDiagnostics, AppError> {
    let (home_setting, active_kind, provider_count, official_account_count) =
        store.read(|state| {
            (
                state.codex.home.clone(),
                state.active.kind,
                state.providers.len(),
                state.official_accounts.len(),
            )
        })?;
    let root = store.root().to_path_buf();
    let home = codex::home(&home_setting);
    let active_model = matches!(active_kind, ActiveKind::Provider)
        .then(|| configured_model(&home))
        .flatten();
    let index = index.inner().clone();
    tokio::task::spawn_blocking(move || {
        build_support_diagnostics(
            &root,
            &home,
            &index,
            active_kind,
            provider_count,
            official_account_count,
            active_model,
        )
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))
}

fn build_support_diagnostics(
    root: &Path,
    home: &Path,
    index: &SessionIndex,
    active_kind: ActiveKind,
    provider_count: usize,
    official_account_count: usize,
    active_model: Option<String>,
) -> SupportDiagnostics {
    let inspection = codex::inspect(home);
    let mut warnings = inspection
        .warnings
        .iter()
        .map(|warning| redact_home_text(warning))
        .collect::<Vec<_>>();
    let (indexed_session_count, session_database_count) = match index.load_with_database_count(home)
    {
        Ok((sessions, database_count)) => (sessions.len(), database_count),
        Err(error) => {
            warnings.push(format!(
                "会话索引读取失败：{}",
                redact_home_text(&error.to_string())
            ));
            (0, 0)
        }
    };
    let usage_database = usage_database_diagnostics(&root.join("usage.sqlite3"), &mut warnings);
    let files = [
        "app.json",
        "connections.json",
        "credentials.json",
        "pricing.json",
        "usage.json",
        "sessions.json",
        "cache.json",
    ]
    .into_iter()
    .map(|name| file_diagnostics(root, name))
    .collect();
    let active_kind = match active_kind {
        ActiveKind::Official => "official",
        ActiveKind::Provider => "provider",
        ActiveKind::None => "none",
    };
    SupportDiagnostics {
        schema_version: 1,
        generated_at: chrono::Utc::now().to_rfc3339(),
        app: SupportAppDiagnostics {
            name: "Codex Tools".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            build_profile: if cfg!(debug_assertions) {
                "debug".into()
            } else {
                "release".into()
            },
        },
        system: SupportSystemDiagnostics {
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            family: std::env::consts::FAMILY.into(),
        },
        paths: SupportPathDiagnostics {
            data_directory: redact_home_path(root),
            codex_home: redact_home_path(home),
            config_file: redact_home_path(&home.join("config.toml")),
        },
        configuration: SupportConfigDiagnostics {
            valid: inspection.valid,
            active_provider: inspection.active_provider,
            managed_provider_present: inspection.managed_provider_present,
            warnings: inspection
                .warnings
                .into_iter()
                .map(|warning| redact_home_text(&warning))
                .collect(),
        },
        connection: SupportConnectionDiagnostics {
            active_kind: active_kind.into(),
            provider_count,
            official_account_count,
            active_model,
        },
        storage: SupportStorageDiagnostics {
            files,
            usage_database,
            session_database_count,
            indexed_session_count,
        },
        network: network::support_diagnostics(),
        warnings,
        privacy: SupportPrivacyDiagnostics {
            home_paths_redacted: true,
            omitted: vec![
                "API keys".into(),
                "OAuth and Cookie tokens".into(),
                "custom header values".into(),
                "account identifiers and email addresses".into(),
                "proxy addresses".into(),
            ],
        },
    }
}

fn file_diagnostics(root: &Path, name: &str) -> SupportFileDiagnostics {
    let path = root.join(name);
    let metadata = fs::metadata(&path).ok();
    SupportFileDiagnostics {
        name: name.into(),
        exists: metadata.is_some(),
        readable: fs::File::open(path).is_ok(),
        size_bytes: metadata.map(|value| value.len()),
    }
}

fn usage_database_diagnostics(
    path: &Path,
    warnings: &mut Vec<String>,
) -> SupportUsageDatabaseDiagnostics {
    let metadata = fs::metadata(path).ok();
    let mut result = SupportUsageDatabaseDiagnostics {
        exists: metadata.is_some(),
        size_bytes: metadata.map(|value| value.len()),
        schema_version: None,
        quick_check: if path.exists() {
            "unavailable".into()
        } else {
            "missing".into()
        },
        event_count: None,
        cursor_count: None,
    };
    if !result.exists {
        return result;
    }
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let Ok(connection) = rusqlite::Connection::open_with_flags(path, flags) else {
        warnings.push("用量数据库无法以只读方式打开。".into());
        return result;
    };
    result.schema_version = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .ok();
    result.quick_check = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .unwrap_or_else(|_| "unavailable".into());
    result.event_count = connection
        .query_row("SELECT COUNT(*) FROM usage_events", [], |row| row.get(0))
        .ok();
    result.cursor_count = connection
        .query_row("SELECT COUNT(*) FROM usage_cursors", [], |row| row.get(0))
        .ok();
    if result.quick_check != "ok" {
        warnings.push(format!("用量数据库快速检查结果：{}", result.quick_check));
    }
    result
}

fn redact_home_path(path: &Path) -> String {
    dirs::home_dir()
        .and_then(|home| {
            path.strip_prefix(home)
                .ok()
                .map(|relative| Path::new("~").join(relative).display().to_string())
        })
        .unwrap_or_else(|| path.display().to_string())
}

fn redact_home_text(value: &str) -> String {
    let Some(home) = dirs::home_dir() else {
        return value.into();
    };
    value.replace(&home.display().to_string(), "~")
}

/// 启动 Codex 桌面应用并默认解锁模型（调试模式启动 + 注入解锁脚本）。
#[tauri::command]
pub(crate) async fn dashboard_launch(
    store: State<'_, Store>,
    activation: State<'_, ActivationLock>,
) -> Result<ModelUnlockResult, AppError> {
    model_unlock::launch_with_debug(&store, &activation).await
}

/// 读取 Codex 应用路径设置（手动配置 + 实际检测结果）。
#[tauri::command]
pub(crate) fn settings_get_codex_app(store: State<Store>) -> Result<CodexAppSetting, AppError> {
    let configured = store.codex_app_setting()?;
    Ok(CodexAppSetting {
        configured: configured.clone(),
        detected: platform::codex_app_path(configured.as_deref())
            .map(|path| path.display().to_string()),
    })
}

/// 保存手动指定的 Codex 应用路径（`.app` 目录或可执行文件）；`None` 恢复自动检测。
#[tauri::command]
pub(crate) fn settings_save_codex_app_path(
    store: State<Store>,
    path: Option<String>,
) -> Result<(), AppError> {
    if let Some(path) = path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        platform::validate_codex_app_path(path).map_err(AppError::InvalidConfig)?;
    }
    store.settings_save_codex_app_path(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn support_paths_hide_the_user_home_directory() {
        let home = dirs::home_dir().expect("测试环境应有用户目录");
        let redacted = redact_home_path(&home.join(".codex/config.toml"));

        assert!(redacted.starts_with('~'));
        assert!(!redacted.contains(&home.display().to_string()));
    }

    #[test]
    fn support_report_contains_health_without_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let home = temp.path().join("codex-home");
        fs::create_dir_all(&home).unwrap();
        UsageLedger::open(&root).unwrap();

        let report = build_support_diagnostics(
            &root,
            &home,
            &SessionIndex::default(),
            ActiveKind::Provider,
            1,
            1,
            Some("gpt-5.6".into()),
        );
        let serialized = serde_json::to_string(&report).unwrap();

        assert_eq!(report.storage.usage_database.quick_check, "ok");
        assert_eq!(report.storage.usage_database.event_count, Some(0));
        assert!(report.privacy.home_paths_redacted);
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("access_token"));
    }

    #[test]
    fn dashboard_projection_identifies_active_provider() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let provider = store
            .connections_save_provider(ProviderProfile {
                id: String::new(),
                name: "Provider".into(),
                base_url: "https://example.test/v1".into(),
                headers: Default::default(),
                timeout_secs: 30,
                enabled: true,
                active: false,
                model: "gpt-5.6-luna".into(),

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
            })
            .unwrap();
        store.activate(&provider.id).unwrap();

        let projection = store.read(project_active).unwrap();

        assert!(matches!(projection.active_kind, ActiveKind::Provider));
        assert_eq!(projection.provider_count, 1);
        assert_eq!(projection.active_provider.as_deref(), Some("Provider"));
        assert_eq!(projection.active_account_id, None);
        assert_eq!(projection.active_account, None);
        assert!(projection.active_quota.is_none());
    }

    #[test]
    fn dashboard_model_is_read_from_actual_codex_config() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("config.toml"), "model = \"api-model\"\n").unwrap();

        assert_eq!(configured_model(temp.path()).as_deref(), Some("api-model"));
    }

    #[test]
    fn dashboard_projection_labels_active_official_account() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let saved = store
            .save_official_account(&StoredOfficialAccount {
                id: String::new(),
                name: "工作日账号".into(),
                remark: "工作账号备注".into(),
                account_id: "workspace".into(),
                email: "person@example.test".into(),
                credential: CodexAuthCredential {
                    auth_mode: "chatgpt".into(),
                    openai_api_key: None,
                    tokens: CodexAuthTokens {
                        id_token: "id-secret".into(),
                        access_token: "access-secret".into(),
                        refresh_token: "refresh-secret".into(),
                        account_id: "workspace".into(),
                    },
                    last_refresh: "2026-07-15T00:00:00Z".into(),
                },
                source: OfficialAccountSource::OpenAiOauth,
                expires_at: None,
                quota: ProviderAccountQuota::default(),
                created_at: 0,
                updated_at: 0,
            })
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let projection = store.read(project_active).unwrap();

        assert!(matches!(projection.active_kind, ActiveKind::Official));
        assert_eq!(
            projection.active_provider.as_deref(),
            Some("OpenAI · 工作账号备注")
        );
        assert_eq!(
            projection.active_account_id.as_deref(),
            Some(saved.id.as_str())
        );
        assert_eq!(projection.active_account.as_deref(), Some("工作账号备注"));
        assert!(projection.active_quota.is_some());
    }
}
