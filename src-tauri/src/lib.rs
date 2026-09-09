mod activation;
mod auth_center;
mod chat_proxy;
mod codex;
mod commands;
mod credential_maintenance;
mod json_store;
mod local_usage;
mod model_unlock;
mod models;
mod models_dev;
mod network;
mod official_pricing;
mod official_quota;
mod official_reset_credits;
mod platform;
mod pricing;
mod provider_http;
mod provider_sync;
mod proxy_import;
mod session_index;
mod state;
mod storage;
mod usage_log;
#[cfg(windows)]
mod webview_privacy;

use auth_center::AuthCenter;
use chat_proxy::ChatProxyRegistry;
use codex::ConfigManager;
use local_usage::UsageLedger;
use models::*;
use session_index::SessionIndex;
use state::{ActivationLock, ApiClient, ResetCreditOperations};
use std::time::Duration;
use storage::Store;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{Manager, WindowEvent};

const TRAY_SHOW_ID: &str = "tray_show";
const TRAY_EXIT_ID: &str = "tray_exit";
const DEFAULT_WINDOW_WIDTH: f64 = 720.0;
const DEFAULT_WINDOW_HEIGHT: f64 = 520.0;
const MIN_WINDOW_WIDTH: f64 = 720.0;
const MIN_WINDOW_HEIGHT: f64 = 520.0;

fn init_tracing() {
    use tracing_subscriber::{
        filter::Targets, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
    };

    let filter = Targets::new().with_target("codex_tools_lib", tracing::Level::INFO);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::CLOSE))
        .try_init();
}

/// 启动即执行一次，之后每分钟检查。实际刷新仍由维护器依据账号状态决定，
/// 因而不会因为轮询而重复消耗轮换型 Refresh Token。
async fn run_credential_maintenance(app: tauri::AppHandle) {
    loop {
        let store = app.state::<Store>();
        let center = app.state::<AuthCenter>();
        let manager = app.state::<ConfigManager>();
        let activation = app.state::<ActivationLock>();
        let proxy = app.state::<ChatProxyRegistry>();
        credential_maintenance::run_once(&store, &center, &manager, &activation, &proxy).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn current_activation(
    store: &Store,
    effective_at_ms: i64,
) -> Result<Option<ActivationRecord>, AppError> {
    store.read(|state| match state.active.kind {
        ActiveKind::Official => state
            .active
            .account_id
            .as_deref()
            .and_then(|id| {
                state
                    .official_accounts
                    .iter()
                    .find(|account| account.id == id)
            })
            .map(|account| activation_for_official(account, effective_at_ms)),
        ActiveKind::Provider => {
            let provider = state
                .active
                .provider_id
                .as_deref()
                .and_then(|id| state.providers.iter().find(|provider| provider.id == id))?;
            Some(activation_for_provider(provider, effective_at_ms))
        }
        ActiveKind::None => None,
    })
}

pub(crate) fn record_current_activation(
    store: &Store,
    ledger: &UsageLedger,
) -> Result<(), AppError> {
    if let Some(activation) = current_activation(store, chrono::Utc::now().timestamp_millis())? {
        ledger.record_activation(activation)?;
    }
    Ok(())
}

fn activation_for_official(
    account: &StoredOfficialAccount,
    effective_at_ms: i64,
) -> ActivationRecord {
    let display_name = account.display_name();
    ActivationRecord {
        effective_at_ms,
        source_kind: UsageSourceKind::Official,
        provider_id: None,
        account_id: Some(canonical_official_account_id(account)),
        model_provider: Some("openai".into()),
        display_name_snapshot: match account.source {
            OfficialAccountSource::OpenAiOauth => display_name.to_owned(),
            OfficialAccountSource::ProxyImport => format!("{display_name} · Cookie 登录"),
        },
        auth_source: Some(
            match account.source {
                OfficialAccountSource::OpenAiOauth => "openai_oauth",
                OfficialAccountSource::ProxyImport => "proxy_import",
            }
            .into(),
        ),
    }
}

fn activation_for_provider(provider: &ProviderProfile, effective_at_ms: i64) -> ActivationRecord {
    ActivationRecord {
        effective_at_ms,
        source_kind: UsageSourceKind::Provider,
        provider_id: Some(provider.id.clone()),
        account_id: None,
        model_provider: Some(codex::MANAGED_PROVIDER_ID.into()),
        display_name_snapshot: provider.name.clone(),
        auth_source: Some("api_key".into()),
    }
}

fn activation_warning(repair: &mut RepairResult, error: AppError) {
    repair
        .warnings
        .push(format!("账号已切换，但用量归属记录失败：{error}"));
}

fn confirm_pending(ledger: &UsageLedger, id: &str, repair: &mut RepairResult) {
    if let Err(error) = ledger.confirm_activation(id) {
        activation_warning(repair, error);
    }
}

fn cancel_pending(ledger: &UsageLedger, id: &str) {
    let _ = ledger.cancel_activation(id);
}

fn begin_activation(
    ledger: &UsageLedger,
    activation: &ActivationRecord,
) -> Result<String, AppError> {
    ledger.begin_activation(activation)
}

fn show_main_window(app: &tauri::AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        return;
    }
    if let Ok(_window) =
        tauri::WebviewWindowBuilder::new(app, "main", tauri::WebviewUrl::App("index.html".into()))
            .title("Codex Tools")
            .inner_size(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT)
            .min_inner_size(MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT)
            .resizable(false)
            .maximizable(false)
            .general_autofill_enabled(false)
            .build()
    {
        #[cfg(windows)]
        let _ = webview_privacy::configure(&_window);
    }
}

pub fn run() {
    init_tracing();
    tracing::info!(operation = "startup", "application starting");
    let store = Store::new().expect("无法初始化应用数据");
    let usage_ledger = UsageLedger::open(store.root()).expect("无法初始化本机用量数据库");
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init());
    // 开发版需要能与已安装版本并存，否则 `tauri dev` 会被发布版的
    // 单实例进程接管并立即退出，看起来像是启动后没有窗口。
    #[cfg(not(debug_assertions))]
    let builder = builder.plugin(tauri_plugin_single_instance::init(|app, _, _| {
        show_main_window(app)
    }));
    let app = builder
        .manage(store)
        .manage(usage_ledger)
        .manage(AuthCenter::default())
        .manage(ConfigManager::default())
        .manage(ActivationLock::default())
        .manage(ApiClient::default())
        .manage(ResetCreditOperations::default())
        .manage(ChatProxyRegistry::default())
        .manage(SessionIndex::default())
        .setup(|app| {
            #[cfg(windows)]
            if let Some(window) = app.get_webview_window("main") {
                webview_privacy::configure(&window)?;
            }
            let show =
                MenuItem::with_id(app, TRAY_SHOW_ID, "显示 Codex Tools", true, None::<&str>)?;
            let exit = MenuItem::with_id(app, TRAY_EXIT_ID, "退出", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &exit])?;
            let tray_icon =
                tauri::image::Image::from_bytes(include_bytes!("../../build/tray.png"))?;
            TrayIconBuilder::new()
                .icon(tray_icon)
                .menu(&menu)
                .tooltip("Codex Tools")
                .show_menu_on_left_click(false)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    TRAY_SHOW_ID => show_main_window(app),
                    TRAY_EXIT_ID => app.exit(0),
                    _ => {}
                })
                .on_tray_icon_event(|tray, event| {
                    if matches!(
                        event,
                        TrayIconEvent::Click {
                            button: MouseButton::Left,
                            button_state: MouseButtonState::Up,
                            ..
                        }
                    ) {
                        show_main_window(tray.app_handle());
                    }
                })
                .build(app)?;
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(run_credential_maintenance(handle));
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::dashboard::dashboard_get,
            commands::dashboard::settings_get_overview,
            commands::dashboard::settings_get_diagnostics,
            commands::providers::connections_list,
            commands::providers::connections_save_provider,
            commands::providers::connections_delete_provider,
            commands::official_accounts::connections_import_cookie,
            commands::official_accounts::connections_login_start,
            commands::official_accounts::connections_login_poll,
            commands::official_accounts::connections_activate_account,
            commands::official_accounts::connections_delete_account,
            commands::official_accounts::connections_delete_accounts,
            commands::official_accounts::connections_update_account_remark,
            commands::official_accounts::connections_update_account_remarks,
            commands::official_accounts::connections_refresh_login,
            commands::official_accounts::connections_open_login_page,
            commands::providers::connections_test_provider,
            commands::providers::connections_list_models,
            commands::providers::connections_refresh_models,
            commands::official_accounts::connections_refresh_quota,
            commands::official_accounts::connections_estimate_quota,
            commands::official_accounts::connections_refresh_all_quota,
            commands::official_accounts::connections_get_reset_credits,
            commands::official_accounts::connections_consume_reset_credit,
            commands::providers::settings_preview_activation,
            commands::providers::settings_apply_activation,
            commands::providers::connections_activate,
            commands::official_accounts::connections_activate_official,
            commands::model_unlock::settings_model_unlock_status,
            commands::model_unlock::settings_unlock_models,
            commands::model_unlock::settings_launch_codex_debug,
            commands::sessions::sessions_scan,
            commands::sessions::sessions_repair,
            commands::sessions::sessions_list,
            commands::dashboard::dashboard_launch,
            commands::dashboard::settings_get_codex_app,
            commands::dashboard::settings_save_codex_app_path,
            commands::usage::usage_get_overview,
            commands::usage::usage_refresh,
            commands::usage::usage_get_trend,
            commands::usage::usage_get_official_pricing,
            commands::usage::usage_refresh_official_pricing,
            commands::usage::usage_list_pricing_rules,
            commands::usage::usage_save_pricing_rule,
            commands::usage::usage_delete_pricing_rule,
            commands::usage::usage_reprice,
        ])
        .build(tauri::generate_context!())
        .expect("Tauri 运行失败");
    app.run(|app, event| match event {
        #[cfg(target_os = "macos")]
        tauri::RunEvent::Reopen {
            has_visible_windows: false,
            ..
        } => show_main_window(app),
        tauri::RunEvent::Exit => {
            let proxy = app.state::<ChatProxyRegistry>();
            tauri::async_runtime::block_on(proxy.stop_all());
        }
        _ => {
            let _ = app;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recreated_window_matches_the_declared_main_window() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        let window = &config["app"]["windows"][0];

        assert_eq!(window["width"].as_f64(), Some(DEFAULT_WINDOW_WIDTH));
        assert_eq!(window["height"].as_f64(), Some(DEFAULT_WINDOW_HEIGHT));
        assert_eq!(window["minWidth"].as_f64(), Some(MIN_WINDOW_WIDTH));
        assert_eq!(window["minHeight"].as_f64(), Some(MIN_WINDOW_HEIGHT));
        assert_eq!(window["resizable"].as_bool(), Some(false));
        assert_eq!(window["maximizable"].as_bool(), Some(false));
    }
}
