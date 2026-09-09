#[cfg(not(test))]
use crate::platform;
use crate::{
    activation::ensure_codex_stopped,
    codex,
    models::{AppError, PageResult, RepairResult, RepairScan, SessionSummary},
    provider_sync,
    session_index::{self, SessionIndex},
    storage::Store,
};
use std::path::PathBuf;
use tauri::State;

const MAX_SESSION_QUERY_CHARS: usize = 256;

/// 不能缓存此判断：Codex 可能在 rollout 与 SQLite 两次写入之间重新启动，或
/// 激活操作已切换到其他连接。修复引擎会在每一次实际写入之前调用它。
fn may_write_now(
    configured_app: &Option<String>,
    home: &std::path::Path,
    expected: &str,
) -> Result<bool, AppError> {
    if codex_app_is_running(configured_app.as_deref()) {
        return Ok(false);
    }
    Ok(provider_sync::configured_provider(home) == expected)
}

fn codex_app_is_running(configured: Option<&str>) -> bool {
    #[cfg(test)]
    {
        let _ = configured;
        false
    }
    #[cfg(not(test))]
    {
        platform::codex_app_running(configured)
    }
}

pub(crate) async fn scan_home(home: PathBuf) -> Result<RepairScan, AppError> {
    tokio::task::spawn_blocking(move || provider_sync::scan(&home))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))
}

pub(crate) async fn repair_home_with_paths(
    store: &Store,
    home: PathBuf,
    target_provider: String,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    let configured_app = store.codex_app_setting()?;
    ensure_codex_stopped(store)?;
    let expected_configured_provider = provider_sync::configured_provider(&home);
    tokio::task::spawn_blocking(move || {
        provider_sync::repair_with_guard_with_paths_for_app(
            &home,
            &target_provider,
            configured_app.as_deref(),
            || may_write_now(&configured_app, &home, &expected_configured_provider),
        )
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))?
}

pub(crate) async fn repair_home_after_activation(
    store: &Store,
    home: PathBuf,
    target_provider: String,
) -> RepairResult {
    repair_home_after_activation_with_paths(store, home, target_provider)
        .await
        .0
}

pub(crate) async fn repair_home_after_activation_with_paths(
    store: &Store,
    home: PathBuf,
    target_provider: String,
) -> (RepairResult, Vec<PathBuf>) {
    let configured_app = match store.codex_app_setting() {
        Ok(configured_app) => configured_app,
        Err(error) => {
            return (
                RepairResult {
                    target_provider,
                    warnings: vec![format!("会话归属未自动修复：{error}")],
                    ..RepairResult::default()
                },
                Vec::new(),
            );
        }
    };
    if let Err(error) = ensure_codex_stopped(store) {
        return (
            RepairResult {
                target_provider,
                warnings: vec![format!("会话归属未自动修复：{error}")],
                ..RepairResult::default()
            },
            Vec::new(),
        );
    }
    let expected_configured_provider = provider_sync::configured_provider(&home);
    let repair_target = target_provider.clone();
    let result = tokio::task::spawn_blocking(move || {
        provider_sync::repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
            &home,
            &repair_target,
            None,
            configured_app.as_deref(),
            || may_write_now(&configured_app, &home, &expected_configured_provider),
        )
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))
    .and_then(|result| result);
    match result {
        Ok((result, affected_paths)) => (result, affected_paths),
        Err(error) => (
            RepairResult {
                target_provider,
                warnings: vec![format!("会话归属未自动修复：{error}")],
                ..RepairResult::default()
            },
            Vec::new(),
        ),
    }
}

#[tauri::command]
#[tracing::instrument(name = "sessions_scan", skip_all)]
pub(crate) async fn sessions_scan(store: State<'_, Store>) -> Result<RepairScan, AppError> {
    let result = scan_home(codex::home(&store.codex_home_setting()?)).await;
    match &result {
        Ok(scan) => tracing::info!(
            operation = "sessions_scan",
            outcome = "ok",
            rollout_files = scan.rollout_files,
            warning_count = scan.warnings.len()
        ),
        Err(_) => tracing::warn!(operation = "sessions_scan", outcome = "error"),
    }
    result
}

#[tauri::command]
#[tracing::instrument(name = "sessions_repair", skip_all)]
pub(crate) async fn sessions_repair(
    store: State<'_, Store>,
    index: State<'_, SessionIndex>,
    target_provider: String,
) -> Result<RepairResult, AppError> {
    let home = codex::home(&store.codex_home_setting()?);
    let index = index.inner().clone();
    let result = repair_home_with_paths(&store, home.clone(), target_provider).await;
    match result {
        Ok((repair, affected_paths)) => {
            if let Err(error) = index.refresh_paths(&home, &affected_paths) {
                index.invalidate();
                let _ = error;
                tracing::warn!(
                    operation = "sessions_repair_index_refresh",
                    outcome = "fallback"
                );
            }
            tracing::info!(
                operation = "sessions_repair",
                outcome = "ok",
                files_modified = repair.files_modified,
                rows_updated = repair.rows_updated,
                warning_count = repair.warnings.len()
            );
            Ok(repair)
        }
        Err(error) => {
            tracing::warn!(operation = "sessions_repair", outcome = "error");
            Err(error)
        }
    }
}

#[tauri::command]
pub(crate) async fn sessions_list(
    store: State<'_, Store>,
    index: State<'_, SessionIndex>,
    query: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
    refresh: Option<bool>,
    status: Option<String>,
) -> Result<PageResult<SessionSummary>, AppError> {
    let home = codex::home(&store.codex_home_setting()?);
    let index = index.inner().clone();
    if refresh.unwrap_or(false) {
        index.invalidate();
    }
    let query = query.unwrap_or_default();
    if query.chars().count() > MAX_SESSION_QUERY_CHARS {
        return Err(AppError::InvalidConfig(
            "会话搜索内容不能超过 256 个字符。".into(),
        ));
    }
    let page = page.unwrap_or(1).max(1);
    let page_size = page_size.unwrap_or(25).clamp(1, 100);
    let archived = match status.as_deref().unwrap_or("active") {
        "active" => false,
        "archived" => true,
        _ => {
            return Err(AppError::InvalidConfig(
                "会话状态只能是 active 或 archived。".into(),
            ));
        }
    };
    tokio::task::spawn_blocking(move || {
        session_index::session_page(&index, &home, &query, page, page_size, archived)
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;

    #[tokio::test]
    async fn post_activation_switch_preserves_rollout_and_thread_history_projection() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let sessions = home.join("sessions/2026/08/24");
        let archived = home.join("archived_sessions/2026/08/24");
        fs::create_dir_all(&sessions).unwrap();
        fs::create_dir_all(&archived).unwrap();
        fs::write(home.join("config.toml"), "model_provider = \"custom\"\n").unwrap();

        let active_rollout = sessions.join("rollout.jsonl");
        let archived_rollout = archived.join("rollout.jsonl");
        let active_bytes = br#"{ "type": "session_meta", "payload": { "id": "active", "model_provider": "openai" } }
{"type":"event_msg","payload":{"type":"user_message","message":"keep active ordinal"}}
{"type":"response_item","payload":{"type":"message","id":"msg_active","role":"assistant","content":[]}}
"#;
        let archived_bytes =
            br#"{"type":"session_meta","payload":{"id":"archived","model_provider":"openai"}}
{"type":"event_msg","payload":{"type":"user_message","message":"keep archived ordinal"}}
"#;
        let thread_history = home.join("thread_history_1.sqlite");
        fs::write(&active_rollout, active_bytes).unwrap();
        fs::write(&archived_rollout, archived_bytes).unwrap();
        let history = Connection::open(&thread_history).unwrap();
        history
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(
                     thread_id TEXT, next_rollout_byte_offset INTEGER, next_rollout_ordinal INTEGER
                 );
                 CREATE TABLE thread_items(
                     thread_id TEXT, turn_id TEXT, item_id TEXT, rollout_ordinal INTEGER, item_json TEXT
                 );
                 CREATE TABLE thread_turns(
                     thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER, rollout_byte_offset INTEGER,
                     rollout_end_ordinal INTEGER, rollout_end_byte_offset INTEGER
                 );
                 INSERT INTO thread_history_projection_state VALUES('active',1,1),('archived',1,1);
                 INSERT INTO thread_items VALUES('active','turn','item',1,'{}'),('archived','turn','item',1,'{}');
                 INSERT INTO thread_turns VALUES('active','turn',1,1,2,2),('archived','turn',1,1,2,2);",
            )
            .unwrap();
        drop(history);
        let history_before = fs::read(&thread_history).unwrap();

        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .settings_save_codex_app_path(Some(
                temp.path().join("missing-codex.exe").display().to_string(),
            ))
            .unwrap();
        let repair = repair_home_after_activation(&store, home.clone(), "custom".into()).await;

        assert!(
            repair.repair_complete,
            "等长切换不应因历史预检产生不完整修复：{:?}",
            repair.warnings
        );
        assert!(
            repair.warnings.is_empty(),
            "等长切换不应读取或干预不相关 projection：{:?}",
            repair.warnings
        );
        assert_eq!(repair.files_scanned, 2);
        assert_eq!(repair.files_modified, 2);
        assert_eq!(repair.session_meta_updated, 2);
        let active_after = fs::read(&active_rollout).unwrap();
        let archived_after = fs::read(&archived_rollout).unwrap();
        // provider 切换只改等长元数据，完整 transcript 与所有 response_item 保留。
        assert_eq!(active_after.len(), active_bytes.len());
        assert_eq!(archived_after.len(), archived_bytes.len());
        assert!(String::from_utf8_lossy(&active_after).contains("model_provider"));
        assert!(String::from_utf8_lossy(&active_after).contains("custom"));
        assert!(String::from_utf8_lossy(&archived_after).contains("model_provider"));
        assert!(String::from_utf8_lossy(&archived_after).contains("custom"));
        assert!(String::from_utf8_lossy(&active_after).contains("keep active ordinal"));
        assert!(String::from_utf8_lossy(&archived_after).contains("keep archived ordinal"));
        assert_ne!(active_after.as_slice(), active_bytes);
        assert_ne!(archived_after.as_slice(), archived_bytes);
        assert_eq!(fs::read(thread_history).unwrap(), history_before);
    }

    #[tokio::test]
    async fn post_activation_repair_rejects_invalid_target() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();

        let result = repair_home_after_activation(
            &store,
            temp.path().join("codex-home"),
            "unsupported".into(),
        )
        .await;

        assert_eq!(result.target_provider, "unsupported");
        assert_eq!(result.files_scanned, 0);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("只能在 OpenAI"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn automatic_repair_reports_missing_configured_cli_and_rolls_back() {
        if std::env::var_os("CODEX_BIN").is_some() {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        fs::write(home.join("config.toml"), "model_provider = \"custom\"\n").unwrap();
        let id = "00000000-0000-7000-8000-0000000000ad";
        let rollout = sessions.join(format!("rollout-2026-09-05T12-00-00-{id}.jsonl"));
        let rollout_before = format!(
            concat!(
                "{{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{}\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"paginated\"}}}}\n",
                "{{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}}}\n",
                "{{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":2,\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"keep\"}}}}\n"
            ),
            id
        );
        fs::write(&rollout, &rollout_before).unwrap();

        let state = home.join("state_5.sqlite");
        let database = Connection::open(&state).unwrap();
        database
            .execute_batch(&format!(
                "CREATE TABLE threads(
                     id TEXT PRIMARY KEY,
                     model_provider TEXT,
                     history_mode TEXT,
                     archived INTEGER,
                     source TEXT
                 );
                INSERT INTO threads VALUES('{id}','codex_tools_openai_relay','paginated',0,NULL);",
            ))
            .unwrap();
        drop(database);

        let history = home.join("thread_history_1.sqlite");
        let database = Connection::open(&history).unwrap();
        database
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(
                     thread_id TEXT PRIMARY KEY,
                     next_rollout_byte_offset INTEGER,
                     next_rollout_ordinal INTEGER
                 );
                 CREATE TABLE thread_items(
                     thread_id TEXT,
                     rollout_ordinal INTEGER,
                     item_json TEXT
                 );
                 CREATE TABLE thread_turns(
                     thread_id TEXT,
                     rollout_ordinal INTEGER,
                     rollout_byte_offset INTEGER,
                     rollout_end_ordinal INTEGER,
                     rollout_end_byte_offset INTEGER
                 );",
            )
            .unwrap();
        drop(database);

        let app = temp.path().join("Custom/Codex.exe");
        fs::create_dir_all(app.parent().unwrap()).unwrap();
        fs::write(&app, b"desktop").unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .settings_save_codex_app_path(Some(app.display().to_string()))
            .unwrap();

        let repair = repair_home_after_activation(&store, home, "custom".into()).await;

        assert!(
            repair
                .warnings
                .iter()
                .any(|warning| warning.contains("无法定位 Codex 内置 CLI")),
            "warnings: {:?}",
            repair.warnings
        );
        assert_eq!(fs::read_to_string(&rollout).unwrap(), rollout_before);
        for database_path in [&state, &history] {
            let database = Connection::open_with_flags(
                database_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap();
            let integrity: String = database
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
        }
        let provider: String = Connection::open(&state)
            .unwrap()
            .query_row(
                "SELECT model_provider FROM threads WHERE id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provider, "codex_tools_openai_relay");
    }
}
