use crate::{
    activation::{
        compensate_activation_failure, ensure_codex_stopped, record_written_model,
        repair_and_publish_provider, sync_active_codex_configuration,
        sync_active_openai_credential,
    },
    chat_proxy::ChatProxyRegistry,
    codex::{self, AppliedConfigPatch, ConfigManager},
    local_usage::UsageLedger,
    models::*,
    models_dev, provider_http,
    session_index::SessionIndex,
    state::{ActivationLock, ApiClient},
    storage::{ProviderSnapshotRevision, ProviderSourceFingerprint, Store},
};
use std::collections::BTreeMap;
use tauri::State;

fn require_api_key(provider: &ProviderProfile) -> Result<&str, AppError> {
    provider
        .api_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
        .ok_or_else(|| AppError::InvalidConfig("此服务还没有 API Key，请先编辑并填写。".into()))
}

#[tauri::command]
pub(crate) async fn connections_list(
    store: State<'_, Store>,
) -> Result<ProviderOverview, AppError> {
    if let Err(error) = store.retry_pending_official_import() {
        tracing::warn!(%error, "待保存的 OpenAI 登录凭据尚未恢复");
    }
    store.provider_overview()
}

#[tauri::command]
pub(crate) async fn connections_save_provider(
    store: State<'_, Store>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    client: State<'_, ApiClient>,
    provider: ProviderSaveInput,
) -> Result<ProviderProfile, AppError> {
    let _model_transaction = activation.2.lock().await;
    let custom_models_explicit = provider.custom_models.is_some();
    let provider = ProviderProfile::from(provider);
    if store.is_active_provider(&provider.id)? {
        ensure_codex_stopped(&store)?;
    }
    let (previous, saved_source, saved_revision, needs_model_refresh, was_active, saved) = {
        let _guard = activation.0.lock().await;
        let (previous, saved) =
            store.connections_save_provider_with_previous(provider, custom_models_explicit)?;
        let saved_source = ProviderSourceFingerprint::from_provider(&saved);
        let saved_revision = ProviderSnapshotRevision::from_provider(&saved);
        let was_active = store.is_active_provider(&saved.id)?;
        // 新连接、模型源变化和从未成功拉取过模型都会得到空缓存。
        let needs_model_refresh = saved.available_models.is_empty()
            && saved
                .api_key
                .as_deref()
                .is_some_and(|key| !key.trim().is_empty());
        // 若正在使用此服务，同步配置时（对 Chat 类型）会自动启动/重启本机转换代理。
        // 地址/Key/空缓存需要先完成 `/models` 刷新，不能把旧模型写给新服务。
        if was_active && !needs_model_refresh {
            if let Err(error) = sync_active_codex_configuration(&store, &manager, &proxy).await {
                let Some(previous) = previous.as_ref() else {
                    return Err(error);
                };
                return Err(restore_active_provider_save(
                    &store,
                    &manager,
                    &proxy,
                    &saved_revision,
                    previous,
                    error,
                )
                .await);
            }
        }
        (
            previous,
            saved_source,
            saved_revision,
            needs_model_refresh,
            was_active,
            saved,
        )
    };
    // 静默获取服务 /models 接口返回的可用模型，并用 models.dev(catalog.json)
    // 精确匹配补充元数据（用户无感知；失败不打扰，保留已有数据）。
    // 放到激活锁之外执行：HTTP 请求可能耗时数秒，不应阻塞其他激活/保存操作。
    if needs_model_refresh {
        let sync_result = match client.current() {
            Ok(client) => {
                sync_provider_models(&client, &store, &saved, Some(&saved_revision)).await
            }
            Err(error) => Err(error),
        };
        match sync_result {
            Ok(synced) if was_active => {
                let _guard = activation.0.lock().await;
                // 刷新期间若用户已切到 OpenAI/其他服务，新 Provider 已是
                // 普通草稿；保留缓存但绝不能重写当前 Codex 配置。
                if !store.is_active_provider(&saved.id)? {
                    return Ok(store.provider(&saved.id)?.redacted());
                }
                if !store.provider_source_matches(&saved.id, &saved_source)? {
                    return Err(AppError::StaleOperation);
                }
                if let Err(error) = sync_active_codex_configuration(&store, &manager, &proxy).await
                {
                    let Some(previous) = previous.as_ref() else {
                        return Err(error);
                    };
                    return Err(restore_active_provider_save(
                        &store,
                        &manager,
                        &proxy,
                        &synced.revision,
                        previous,
                        error,
                    )
                    .await);
                }
            }
            Err(error) if was_active => {
                let _guard = activation.0.lock().await;
                // 刷新期间已切走时按未激活草稿处理：保留用户保存的数据，
                // 后续真正激活时会重新获取模型并在无缓存时明确拒绝。
                if !store.is_active_provider(&saved.id)? {
                    return Ok(store.provider(&saved.id)?.redacted());
                }
                let Some(previous) = previous.as_ref() else {
                    return Err(error);
                };
                return Err(rollback_active_save_after_model_failure(
                    &store,
                    &manager,
                    &proxy,
                    &saved_revision,
                    previous,
                    error,
                )
                .await);
            }
            Ok(_) => {}
            // 未激活的服务允许先保存为草稿；激活时会重试，且无缓存会明确拒绝。
            Err(_) => {}
        }
    }
    Ok(store.provider(&saved.id)?.redacted())
}

async fn rollback_active_save_after_model_failure(
    store: &Store,
    manager: &ConfigManager,
    proxy: &ChatProxyRegistry,
    saved_revision: &ProviderSnapshotRevision,
    previous: &ProviderProfile,
    error: AppError,
) -> AppError {
    // 另一个更新已改变 Provider source。旧请求绝不能回滚，否则会
    // 撤销新来源并覆盖真正的最新保存。
    if matches!(error, AppError::StaleOperation) {
        return error;
    }
    let failure = AppError::InvalidConfig(format!(
        "无法从更新后的服务获取模型列表，已保留原连接：{error}"
    ));
    restore_active_provider_save(store, manager, proxy, saved_revision, previous, failure).await
}

/// 在持有 ActivationLock 时恢复 active Provider 的完整旧快照，并把 Codex
/// 配置同步回旧连接。完整 snapshot revision 的用户态检查失败表示已有更新完成，
/// 绝不能用旧请求覆盖 timeout/enabled/模型缓存等后续提交。
async fn restore_active_provider_save(
    store: &Store,
    manager: &ConfigManager,
    proxy: &ChatProxyRegistry,
    expected_revision: &ProviderSnapshotRevision,
    previous: &ProviderProfile,
    original_error: AppError,
) -> AppError {
    match store.restore_provider_snapshot_if_revision_matches(expected_revision, previous) {
        Ok(false) => AppError::StaleOperation,
        Err(restore) => AppError::Internal(format!(
            "{original_error}；恢复原服务数据失败，请重新选择连接：{restore}"
        )),
        Ok(true) => match sync_active_codex_configuration(store, manager, proxy).await {
            Ok(()) => original_error,
            Err(restore) => AppError::Internal(format!(
                "{original_error}；原服务配置恢复失败，请重新选择连接：{restore}"
            )),
        },
    }
}

/// 提交第三方配置后立即记录实际写入的 API 模型。记录失败时 Store 仍指向
/// 原连接，因此可以用统一补偿路径恢复原配置，避免留下无法安全清理的 model。
async fn apply_provider_configuration_and_record(
    store: &Store,
    manager: &ConfigManager,
    proxy: &ChatProxyRegistry,
    home: &std::path::Path,
    operation_id: &str,
) -> Result<ProviderActivationRollback, AppError> {
    let previous_managed_model = store.last_managed_model()?;
    let applied = match manager.apply_checked(operation_id, || ensure_codex_stopped(store)) {
        Ok(applied) => applied,
        Err(error) => {
            return Err(compensate_activation_failure(store, manager, proxy, error).await);
        }
    };
    if let Err(error) = record_written_model(store, home) {
        let rollback = ProviderActivationRollback {
            applied,
            previous_managed_model,
        };
        return Err(rollback_provider_activation(store, manager, proxy, rollback, error).await);
    }
    Ok(ProviderActivationRollback {
        applied,
        previous_managed_model,
    })
}

struct ProviderActivationRollback {
    applied: AppliedConfigPatch,
    previous_managed_model: Option<String>,
}

async fn rollback_provider_activation(
    store: &Store,
    manager: &ConfigManager,
    proxy: &ChatProxyRegistry,
    rollback: ProviderActivationRollback,
    original_error: AppError,
) -> AppError {
    let files = manager.rollback_applied(rollback.applied);
    let managed_model = store.save_last_managed_model(rollback.previous_managed_model);
    match (files, managed_model) {
        (Ok(()), Ok(())) => original_error,
        (files, managed_model) => {
            let rollback_error = files
                .err()
                .or_else(|| managed_model.err())
                .unwrap_or_else(|| AppError::Internal("未知回滚错误".into()));
            compensate_activation_failure(
                store,
                manager,
                proxy,
                AppError::Internal(format!(
                    "{original_error}；第三方配置回滚失败，请手动检查 Codex 配置：{rollback_error}"
                )),
            )
            .await
        }
    }
}

#[tauri::command]
pub(crate) async fn connections_delete_provider(
    store: State<'_, Store>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    id: String,
) -> Result<(), AppError> {
    let _model_transaction = activation.2.lock().await;
    let _guard = activation.0.lock().await;
    store.connections_delete_provider(&id)?;
    proxy.stop(&id).await;
    Ok(())
}

#[tauri::command]
pub(crate) async fn connections_test_provider(
    store: State<'_, Store>,
    client: State<'_, ApiClient>,
    id: String,
) -> Result<ProviderTestResult, AppError> {
    let mut provider = store.provider(&id)?;
    provider.normalize_and_validate()?;
    let key = require_api_key(&provider)?;
    let endpoint = provider_http::models_endpoint(&provider.base_url);
    let mut request = client
        .current()?
        .get(&endpoint)
        .headers(provider_http::custom_headers(&provider)?);
    request = request.bearer_auth(key);
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(provider.timeout_secs),
        request.send(),
    )
    .await
    .map_err(|_| AppError::InvalidConfig("连接超时，请检查网络和 API 地址后重试。".into()))?
    .map_err(|error| {
        AppError::InvalidConfig(format!("无法连接到服务，请检查网络和 API 地址：{error}"))
    })?;
    let status_code = response.status();
    let status = status_code.as_u16();
    let ok = provider_http::provider_test_succeeded(status_code);
    Ok(ProviderTestResult {
        ok,
        status,
        endpoint,
        message: if ok {
            if matches!(provider.api_type, ProviderApiType::Chat) {
                "模型列表接口可以访问，本机转换代理可正常接入该服务。".into()
            } else {
                "模型列表接口可以访问，Codex 可直接从此服务读取模型。".into()
            }
        } else {
            format!("连接测试未通过（HTTP {status}），请检查 API 地址、API Key 和服务状态。")
        },
        suggest_v1: status == 404 && !provider.base_url.ends_with("/v1"),
    })
}

#[tauri::command]
pub(crate) async fn connections_list_models(
    store: State<'_, Store>,
    client: State<'_, ApiClient>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    id: String,
) -> Result<Vec<String>, AppError> {
    let _model_transaction = activation.2.lock().await;
    let (mut provider, starting_revision, was_active) =
        store.provider_model_refresh_snapshot(&id)?;
    if was_active {
        ensure_codex_stopped(&store)?;
    }
    provider.normalize_and_validate()?;
    require_api_key(&provider)?;
    // 抓取 `/models` 可用模型并保存（含 models.dev 精确匹配的元数据）。
    let synced = sync_provider_models(
        &client.current()?,
        &store,
        &provider,
        was_active.then_some(&starting_revision),
    )
    .await?;
    finish_provider_model_refresh(
        &store,
        &manager,
        &activation,
        &proxy,
        was_active,
        &provider,
        &synced.revision,
    )
    .await?;
    Ok(synced.models)
}

/// 手动刷新当前激活服务商的模型列表（第三方 API 更新模型后使用）。
#[tauri::command]
pub(crate) async fn connections_refresh_models(
    store: State<'_, Store>,
    client: State<'_, ApiClient>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
) -> Result<Vec<String>, AppError> {
    let _model_transaction = activation.2.lock().await;
    refresh_active_models(
        &store,
        || client.current(),
        &activation,
        &proxy,
        Some(&manager),
    )
    .await?
    .ok_or_else(|| AppError::InvalidConfig("当前不是第三方 API 服务，无需刷新模型列表。".into()))
}

pub(crate) async fn run_model_refresh(app: tauri::AppHandle) {
    use tauri::{Emitter, Manager};
    let mut interval = model_refresh_interval();
    loop {
        interval.tick().await;
        let activation = app.state::<ActivationLock>();
        let Ok(_guard) = activation.2.try_lock() else {
            continue;
        };
        match refresh_active_models(
            &app.state::<Store>(),
            || app.state::<ApiClient>().current(),
            &activation,
            &app.state::<ChatProxyRegistry>(),
            None,
        )
        .await
        {
            Ok(Some(_)) => {
                let _ = app.emit("provider-models-refreshed", ());
            }
            Ok(None) => {}
            Err(_) => tracing::warn!(operation = "background_model_refresh", outcome = "error"),
        }
    }
}

fn model_refresh_interval() -> tokio::time::Interval {
    let period = std::time::Duration::from_secs(10 * 60);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

async fn refresh_active_models(
    store: &Store,
    client: impl FnOnce() -> Result<reqwest::Client, AppError>,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    manual: Option<&ConfigManager>,
) -> Result<Option<Vec<String>>, AppError> {
    let (_, Some(provider)) = store.active_provider_and_home()? else {
        return Ok(None);
    };
    if manual.is_none() && (!provider.enabled || require_api_key(&provider).is_err()) {
        return Ok(None);
    }
    if let Err(error) = ensure_codex_stopped(store) {
        if manual.is_none() && matches!(&error, AppError::InvalidConfig(_)) {
            return Ok(None);
        }
        return Err(error);
    }
    let starting_revision = ProviderSnapshotRevision::from_provider(&provider);
    let synced =
        sync_provider_models(&client()?, store, &provider, Some(&starting_revision)).await?;
    if let Some(manager) = manual {
        finish_provider_model_refresh(
            store,
            manager,
            activation,
            proxy,
            true,
            &provider,
            &synced.revision,
        )
        .await?;
    } else {
        let changed = synced.revision != starting_revision;
        let _guard = activation.0.lock().await;
        let (home, active) = store.active_provider_and_home()?;
        let Some(current) = active.filter(|active| active.id == provider.id) else {
            return Ok(None);
        };
        if ProviderSnapshotRevision::from_provider(&current) != synced.revision {
            return Err(AppError::StaleOperation);
        }
        let url = match current.api_type {
            ProviderApiType::Responses => current.base_url.clone(),
            ProviderApiType::Chat => proxy.base_url(),
        };
        match codex::refresh_background_model_files(store, &home, &current, &url, changed) {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) if changed => {
                return Err(rollback_model_refresh(
                    store,
                    &provider,
                    &synced.revision,
                    error,
                ));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(Some(synced.models))
}

#[allow(clippy::too_many_arguments)] // Keep the refresh transaction dependencies explicit.
async fn finish_provider_model_refresh(
    store: &Store,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    was_active_at_start: bool,
    previous: &ProviderProfile,
    refreshed_revision: &ProviderSnapshotRevision,
) -> Result<(), AppError> {
    // 从未激活状态开始的刷新只是更新缓存。即使网络等待期间该服务被激活，
    // 激活事务也会自行刷新/写配置；本请求不得在失败时回滚其已消费的缓存。
    if !was_active_at_start {
        return Ok(());
    }
    sync_active_provider_configuration(
        store,
        manager,
        activation,
        proxy,
        &previous.id,
        previous,
        refreshed_revision,
    )
    .await
}

/// 模型列表发生变化后，仅当该服务仍为当前连接时重写 Codex 配置和模型目录。
/// HTTP 请求在锁外完成，这里只串行化最终状态确认与文件提交。
#[allow(clippy::too_many_arguments)] // Keep the activation transaction dependencies explicit.
async fn sync_active_provider_configuration(
    store: &Store,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
    provider_id: &str,
    previous: &ProviderProfile,
    refreshed_revision: &ProviderSnapshotRevision,
) -> Result<(), AppError> {
    let _guard = activation.0.lock().await;
    if store.is_active_provider(provider_id)? {
        if let Err(error) = sync_active_codex_configuration(store, manager, proxy).await {
            return Err(rollback_model_refresh(
                store,
                previous,
                refreshed_revision,
                error,
            ));
        }
    }
    Ok(())
}

fn rollback_model_refresh(
    store: &Store,
    previous: &ProviderProfile,
    revision: &ProviderSnapshotRevision,
    error: AppError,
) -> AppError {
    match store.restore_provider_snapshot_if_revision_matches(revision, previous) {
        Ok(true) => error,
        Ok(false) => AppError::StaleOperation,
        Err(rollback) => AppError::Internal(format!(
            "{error}；模型缓存回滚失败，请重新刷新当前服务：{rollback}"
        )),
    }
}

/// 抓取服务 `/models` 接口的可用模型，并用 models.dev（catalog.json）**精确
/// 匹配**（id 完全一致）补充展示名/上下文窗口/简介后保存；返回可用模型列表。
struct SyncedProviderModels {
    models: Vec<String>,
    revision: ProviderSnapshotRevision,
}

fn validate_fresh_activation_models(
    refresh_error: Option<AppError>,
    provider: &ProviderProfile,
) -> Result<(), AppError> {
    let selected = provider.selected_models.as_deref();
    let custom_model_selected = provider.custom_models.iter().any(|model| {
        !model.trim().is_empty()
            && selected.is_none_or(|selected| selected.iter().any(|value| value == model))
    });
    let explicitly_custom_only = selected.is_some_and(|selected| {
        !selected.is_empty()
            && selected.iter().all(|model| {
                provider
                    .custom_models
                    .iter()
                    .any(|custom| custom == model && !custom.trim().is_empty())
            })
    });
    if let Some(error) = refresh_error {
        if matches!(error, AppError::StaleOperation) {
            return Err(error);
        }
        // A manually configured catalog is the explicit fallback for services
        // that do not expose /models. Never trust stale API models after a
        // failed refresh, but allow a custom-only provider to activate.
        if custom_model_selected && (provider.available_models.is_empty() || explicitly_custom_only)
        {
            return Ok(());
        }
        return Err(AppError::InvalidConfig(format!(
            "无法获取此服务的最新模型，连接未切换；已保留原有模型缓存：{error}"
        )));
    }
    let available_model_selected = provider.available_models.iter().any(|model| {
        !model.trim().is_empty()
            && selected.is_none_or(|selected| selected.iter().any(|value| value == model))
    });
    if !available_model_selected && !custom_model_selected {
        return Err(AppError::InvalidConfig(
            "此服务没有可用模型，无法激活。请检查 /models 接口后重试".into(),
        ));
    }
    Ok(())
}

async fn sync_provider_models(
    client: &reqwest::Client,
    store: &Store,
    provider: &ProviderProfile,
    expected_revision: Option<&ProviderSnapshotRevision>,
) -> Result<SyncedProviderModels, AppError> {
    let expected_source = ProviderSourceFingerprint::from_provider(provider);
    // /models 和 models.dev 并行请求，减少网络延迟叠加。
    let (details_result, dev_meta_result) = tokio::join!(
        provider_http::fetch_model_details(client, provider),
        models_dev::fetch_provider_meta(client, &provider.base_url, &provider.name),
    );
    let details = details_result?;
    let mut models: Vec<String> = details.iter().map(|detail| detail.id.clone()).collect();
    models.sort();
    models.dedup();
    let windows = details
        .iter()
        .filter_map(|detail| {
            detail
                .context_window
                .map(|window| (detail.id.clone(), window))
        })
        .collect::<BTreeMap<_, _>>();
    let meta = merge_provider_model_metadata(&details, dev_meta_result.as_ref().ok());
    let Some(revision) = store.update_provider_models_if_source_matches(
        &provider.id,
        &expected_source,
        expected_revision,
        models.clone(),
        windows,
        meta,
    )?
    else {
        return Err(AppError::StaleOperation);
    };
    Ok(SyncedProviderModels { models, revision })
}

fn merge_provider_model_metadata(
    details: &[provider_http::FetchedModelDetail],
    dev_meta: Option<&BTreeMap<String, ProviderModelsDevMeta>>,
) -> BTreeMap<String, ProviderModelsDevMeta> {
    // 先并入 /models 返回的简介（服务商自己的数据优先），再补 models.dev 元数据。
    let mut meta = details
        .iter()
        .filter(|detail| {
            detail.description.is_some()
                || detail.reasoning_levels.is_some()
                || detail.default_reasoning_level.is_some()
                || detail.service_tiers.is_some()
        })
        .map(|detail| {
            (
                detail.id.clone(),
                ProviderModelsDevMeta {
                    name: None,
                    context_window: None,
                    description: detail.description.clone(),
                    supported_reasoning_levels: detail.reasoning_levels.clone(),
                    default_reasoning_level: detail.default_reasoning_level.clone(),
                    service_tiers: detail.service_tiers.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(dev_meta) = dev_meta {
        // 按 /models 返回的原始 id 解析 models.dev 元数据（兼容纯 id / 厂商前缀 / 大小写）。
        for detail in details {
            let id = &detail.id;
            let Some(info) = models_dev::lookup_model(dev_meta, id) else {
                continue;
            };
            let entry = meta.entry(id.clone()).or_default();
            if entry.name.is_none() {
                entry.name = info.name.clone();
            }
            if entry.context_window.is_none() {
                entry.context_window = info.context_window;
            }
            if entry.description.is_none() {
                entry.description = info.description.clone();
            }
        }
    }
    meta
}

#[tauri::command]
pub(crate) async fn settings_preview_activation(
    store: State<'_, Store>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
) -> Result<ConfigPatchPreview, AppError> {
    preview_activation_in_transaction(&store, &manager, &activation, &proxy).await
}

async fn preview_activation_in_transaction(
    store: &Store,
    manager: &ConfigManager,
    activation: &ActivationLock,
    proxy: &ChatProxyRegistry,
) -> Result<ConfigPatchPreview, AppError> {
    // 与 save/refresh/activate 使用相同锁序。必须先取得两把锁再读取
    // Provider 和 config/auth/catalog，否则旧 Provider 可能配上新文件快照，
    // 后续 apply 的用户态快照检查仍会错误通过。
    let _model_transaction = activation.2.lock().await;
    let _activation = activation.0.lock().await;
    let (home, provider) = store.active_provider_and_home()?;
    let provider = provider
        .ok_or_else(|| AppError::InvalidConfig("请先添加并启用一个第三方 API 服务。".into()))?;
    let target = crate::chat_proxy::effective_base_url(&provider, proxy).await?;
    manager.preview_custom(&home, &provider, &target)
}

#[tauri::command]
pub(crate) async fn settings_apply_activation(
    store: State<'_, Store>,
    manager: State<'_, ConfigManager>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    operation_id: String,
) -> Result<(), AppError> {
    let activation_operation = activation.begin_operation();
    let _model_transaction = activation.2.lock().await;
    let _guard = activation.0.lock().await;
    if !activation.is_current(activation_operation) {
        return Err(AppError::StaleOperation);
    }
    ensure_codex_stopped(&store)?;
    let (home, provider) = store.active_provider_and_home()?;
    let provider = provider.ok_or(AppError::StaleOperation)?;
    manager.check_preview_provider(&operation_id, &home, &provider)?;
    let previous_managed_model = store.last_managed_model()?;
    let applied = manager.apply_checked(&operation_id, || ensure_codex_stopped(&store))?;
    // 写入成功后记录 config.toml 当前的服务模型，供切换到 OpenAI 时精确清除。
    let finish = async {
        record_written_model(&store, &home)?;
        if provider.api_type == crate::models::ProviderApiType::Chat {
            repair_and_publish_provider(&store, &proxy, &provider, home).await?;
        } else {
            store.confirm_chat_route(&provider.id, None)?;
            proxy.publish(None).await;
        }
        Ok::<_, AppError>(())
    }
    .await;
    if let Err(error) = finish {
        let files = manager.rollback_applied(applied);
        let managed_model = store.save_last_managed_model(previous_managed_model);
        return match (files, managed_model) {
            (Ok(()), Ok(())) => Err(error),
            (files, managed_model) => Err(AppError::Internal(format!(
                "{error}；Codex 配置回滚失败：{}",
                files
                    .err()
                    .or_else(|| managed_model.err())
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "未知错误".into())
            ))),
        };
    }
    Ok(())
}

#[tauri::command]
#[allow(clippy::too_many_arguments)] // Tauri injects each managed state separately.
pub(crate) async fn connections_activate(
    store: State<'_, Store>,
    manager: State<'_, ConfigManager>,
    ledger: State<'_, UsageLedger>,
    activation: State<'_, ActivationLock>,
    proxy: State<'_, ChatProxyRegistry>,
    client: State<'_, ApiClient>,
    index: State<'_, SessionIndex>,
    id: String,
) -> Result<RepairResult, AppError> {
    let activation_operation = activation.begin_operation();
    let _model_transaction = activation.2.lock().await;
    if !activation.is_current(activation_operation) {
        return Err(AppError::StaleOperation);
    }
    ensure_codex_stopped(&store)?;
    // 先在激活锁之外刷新模型，避免网络请求阻塞其他切换。激活属于高影响
    // 操作，必须以本次 `/models` 实时验证为准，不能用旧缓存掩盖失效的 Key
    // 或不可达的服务。
    let mut candidate = store.provider(&id)?;
    candidate.normalize_and_validate()?;
    if !candidate.enabled {
        return Err(AppError::InvalidConfig(
            "所选第三方 API 服务已停用，请检查后重试。".into(),
        ));
    }
    require_api_key(&candidate)?;
    let refresh_error = sync_provider_models(&client.current()?, &store, &candidate, None)
        .await
        .err();
    let refreshed = store.provider(&id)?;
    validate_fresh_activation_models(refresh_error, &refreshed)?;
    let repair = {
        let _guard = activation.0.lock().await;
        if !activation.is_current(activation_operation) {
            return Err(AppError::StaleOperation);
        }
        ensure_codex_stopped(&store)?;
        let mut provider = store.provider(&id)?;
        provider.normalize_and_validate()?;
        if !provider.enabled {
            return Err(AppError::InvalidConfig(
                "所选第三方 API 服务已停用，请检查后重试。".into(),
            ));
        }
        require_api_key(&provider)?;
        let target = crate::chat_proxy::effective_base_url(&provider, &proxy).await?;
        if !activation.is_current(activation_operation) {
            return Err(AppError::StaleOperation);
        }
        let home = codex::home(&store.codex_home_setting()?);
        sync_active_openai_credential(&store, &home)?;
        // 配置文件中的任意第三方 provider 都会归一为 "custom"，但旧 rollout
        // 仍可能保留已删除的 codex_tools_openai_relay。始终调用修复引擎；
        // 正常 custom -> custom 历史会被识别为目标路由并保持零写入。
        let preview = manager.preview_custom(&home, &provider, &target)?;
        let pending_id = crate::begin_activation(
            &ledger,
            &crate::activation_for_provider(&provider, chrono::Utc::now().timestamp_millis()),
        )?;
        let result = async {
            let previous_active = store.read(|state| state.active.clone())?;
            let rollback = apply_provider_configuration_and_record(
                &store,
                &manager,
                &proxy,
                &home,
                &preview.operation_id,
            )
            .await?;
            if let Err(error) = store.activate(&id) {
                return Err(rollback_provider_activation(
                    &store, &manager, &proxy, rollback, error,
                )
                .await);
            }
            // 转换代理保持运行：Codex 会缓存配置里的地址，端口必须在本机会话内
            // 保持稳定，切回其他服务再切回来时才能继续使用同一端口。
            let finish = repair_and_publish_provider(&store, &proxy, &provider, home.clone()).await;
            if let Err(rollback_error) = finish {
                if let Err(restore) = store.restore_active_state(&previous_active) {
                    return Err(rollback_provider_activation(
                        &store,
                        &manager,
                        &proxy,
                        rollback,
                        AppError::Internal(format!(
                            "{rollback_error}；恢复原连接状态失败：{restore}"
                        )),
                    )
                    .await);
                }
                return Err(rollback_provider_activation(
                    &store,
                    &manager,
                    &proxy,
                    rollback,
                    rollback_error,
                )
                .await);
            }
            finish
        }
        .await;
        match result {
            Ok((mut repair, affected_paths)) => {
                crate::confirm_pending(&ledger, &pending_id, &mut repair);
                if let Err(error) = index.refresh_paths(&home, &affected_paths) {
                    index.invalidate();
                    eprintln!("连接切换后定向刷新会话索引失败，已回退全量重建：{error}");
                }
                Ok(repair)
            }
            Err(error) => {
                crate::cancel_pending(&ledger, &pending_id);
                Err(error)
            }
        }
    }?;
    Ok(repair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activation::sync_active_codex_configuration;

    fn provider(id: &str, base_url: &str, model: &str) -> ProviderProfile {
        ProviderProfile {
            id: id.into(),
            name: "Provider".into(),
            base_url: base_url.into(),
            headers: BTreeMap::new(),
            timeout_secs: 30,
            enabled: true,
            active: false,
            model: String::new(),
            model_context_windows: BTreeMap::new(),
            context_window_override: None,
            available_models: vec![model.into()],
            selected_models: None,
            custom_models: Default::default(),
            models_dev_meta: BTreeMap::new(),
            api_type: ProviderApiType::Responses,
            api_key: Some("secret".into()),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn direct_target(base_url: &str) -> crate::chat_proxy::ActivationTarget {
        crate::chat_proxy::ActivationTarget {
            base_url: base_url.into(),
            proxy_api_key: None,
        }
    }

    #[tokio::test]
    async fn background_refresh_updates_only_the_active_catalog_and_skips_unchanged_models() {
        use futures_util::FutureExt;
        let mut timer = model_refresh_interval();
        assert_eq!(timer.period(), std::time::Duration::from_secs(600));
        assert!(timer.tick().now_or_never().is_none());
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let lock = ActivationLock::default();
        let proxy = ChatProxyRegistry::default();
        assert!(
            refresh_active_models(&store, || panic!("no active source"), &lock, &proxy, None)
                .await
                .unwrap()
                .is_none()
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = hits.clone();
        let router = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(move || {
                let n = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if n == 0 {
                        return Err(axum::http::StatusCode::BAD_GATEWAY);
                    }
                    let model = if n >= 5 {
                        "replacement"
                    } else {
                        "catalog-fixture"
                    };
                    Ok(axum::Json(serde_json::json!({"data":[{"id":model}]})))
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http = reqwest::Client::builder()
            .no_proxy()
            .resolve("models.dev", address)
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap();
        store
            .connections_save_provider(provider(
                "scheduled",
                &format!("http://{address}/v1"),
                "old",
            ))
            .unwrap();
        store.activate("scheduled").unwrap();
        let config = home.join("config.toml");
        std::fs::write(&config, "# user configuration").unwrap();
        assert!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .is_err()
        );
        assert_eq!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .unwrap(),
            Some(vec!["catalog-fixture".to_owned()])
        );
        assert!(home.join("model-catalogs/codex-tools.json").is_file());
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "# user configuration"
        );
        assert!(!home.join("auth.json").exists());
        assert!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .unwrap()
                .is_none()
        );
        // Earlier versions refreshed the catalog but left a retired default on disk.
        let original = format!(
            "model_provider = 'custom'\nmodel = 'old' # keep this comment\nmodel_catalog_json = 'model-catalogs/codex-tools.json'\n[model_providers.custom]\nname = 'Provider'\nbase_url = 'http://{address}/v1'\n"
        );
        std::fs::write(&config, &original).unwrap();
        std::fs::write(home.join("auth.json"), b"private-auth-fixture").unwrap();
        store.save_last_managed_model(Some("old".into())).unwrap();
        assert!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .unwrap()
                .is_some()
        );
        let repaired = std::fs::read_to_string(&config).unwrap();
        assert_eq!(repaired, original.replace("'old'", "\"catalog-fixture\""));
        assert_eq!(
            store.last_managed_model().unwrap().as_deref(),
            Some("catalog-fixture")
        );
        assert_eq!(
            std::fs::read(home.join("auth.json")).unwrap(),
            b"private-auth-fixture"
        );
        store
            .save_last_managed_model(Some("retired".into()))
            .unwrap();
        let manual = repaired.replace("\"catalog-fixture\"", "'my-own-choice'");
        std::fs::write(&config, &manual).unwrap();
        assert!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(std::fs::read_to_string(&config).unwrap(), manual);
        std::fs::write(&config, &repaired).unwrap();
        store
            .save_last_managed_model(Some("catalog-fixture".into()))
            .unwrap();
        assert_eq!(
            refresh_active_models(&store, || Ok(http.clone()), &lock, &proxy, None)
                .await
                .unwrap(),
            Some(vec!["replacement".into()])
        );
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            repaired.replace("\"catalog-fixture\"", "\"replacement\"")
        );
        assert_eq!(
            store.last_managed_model().unwrap().as_deref(),
            Some("replacement")
        );
        let catalog: serde_json::Value = serde_json::from_slice(
            &std::fs::read(home.join("model-catalogs/codex-tools.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(catalog["models"][0]["slug"], "replacement");
        assert_eq!(
            std::fs::read(home.join("auth.json")).unwrap(),
            b"private-auth-fixture"
        );
        store
            .update(|state| {
                state.providers[0].enabled = false;
                Ok(())
            })
            .unwrap();
        assert!(
            refresh_active_models(&store, || panic!("disabled source"), &lock, &proxy, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 6);
        server.abort();
    }

    #[tokio::test]
    async fn fast_mode_provider_declarations_survive_storage_and_catalog() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"data": [{
                    "id": "m",
                    "service_tiers": [{
                        "id": "priority", "name": "Fast", "description": "Provider tier"
                    }],
                    "default_service_tier": "priority"
                }]}))
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let temp = tempfile::tempdir().unwrap();
        let store_path = temp.path().join("data");
        let store = Store::open(store_path.clone()).unwrap();
        let saved = store
            .connections_save_provider(provider("p", &format!("http://{address}/v1"), "m"))
            .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let details = provider_http::fetch_model_details(&client, &saved)
            .await
            .unwrap();
        server.abort();
        let meta = merge_provider_model_metadata(&details, None);
        assert!(
            store
                .update_provider_models_if_source_matches(
                    &saved.id,
                    &ProviderSourceFingerprint::from_provider(&saved),
                    Some(&ProviderSnapshotRevision::from_provider(&saved)),
                    vec!["m".into()],
                    BTreeMap::new(),
                    meta,
                )
                .unwrap()
                .is_some()
        );
        drop(store);
        let reopened = Store::open(store_path).unwrap();
        let persisted = reopened.provider(&saved.id).unwrap();
        let catalog = crate::model_unlock::build_model_catalog_with_windows_for_provider(
            &persisted,
            temp.path(),
        );
        let json = serde_json::to_value(catalog).unwrap();
        assert_eq!(
            json[0]["service_tiers"],
            serde_json::json!([{
                "id": "priority", "name": "Fast", "description": "Provider tier"
            }])
        );
        assert_eq!(
            json[0]["additional_speed_tiers"],
            serde_json::json!(["fast"])
        );
        assert_eq!(json[0]["default_service_tier"], serde_json::Value::Null);

        // A successful refresh that omits the declaration must remove old support,
        // even when the enrichment source offers a tier for the same model.
        let mut withdrawn = details;
        withdrawn[0].service_tiers = None;
        let dev_meta = BTreeMap::from([(
            "m".into(),
            serde_json::from_value(serde_json::json!({
                "name": "Model M",
                "serviceTiers": [{"id": "fast", "name": "Fast", "description": ""}]
            }))
            .unwrap(),
        )]);
        let meta = merge_provider_model_metadata(&withdrawn, Some(&dev_meta));
        assert!(
            reopened
                .update_provider_models_if_source_matches(
                    &persisted.id,
                    &ProviderSourceFingerprint::from_provider(&persisted),
                    Some(&ProviderSnapshotRevision::from_provider(&persisted)),
                    vec!["m".into()],
                    BTreeMap::new(),
                    meta,
                )
                .unwrap()
                .is_some()
        );
        let refreshed = reopened.provider(&persisted.id).unwrap();
        assert_eq!(
            refreshed.models_dev_meta["m"].name.as_deref(),
            Some("Model M")
        );
        assert!(refreshed.models_dev_meta["m"].service_tiers.is_none());
        let catalog = crate::model_unlock::build_model_catalog_with_windows_for_provider(
            &refreshed,
            temp.path(),
        );
        assert!(catalog[0].service_tiers.is_empty());
        assert!(catalog[0].additional_speed_tiers.is_empty());
    }

    #[test]
    fn reasoning_sync_metadata_survives_storage_and_models_dev_enrichment() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let saved = store
            .connections_save_provider(provider("p", "https://example.test/v1", "m"))
            .unwrap();
        let details = vec![provider_http::FetchedModelDetail {
            id: "m".into(),
            context_window: None,
            description: None,
            reasoning_levels: Some(vec!["low".into(), "max".into()]),
            default_reasoning_level: Some("max".into()),
            service_tiers: None,
        }];
        let dev_meta = BTreeMap::from([(
            "m".into(),
            ProviderModelsDevMeta {
                name: Some("Model M".into()),
                context_window: Some(256_000),
                supported_reasoning_levels: Some(vec!["ultra".into()]),
                ..Default::default()
            },
        )]);
        let meta = merge_provider_model_metadata(&details, Some(&dev_meta));
        assert!(
            store
                .update_provider_models_if_source_matches(
                    &saved.id,
                    &ProviderSourceFingerprint::from_provider(&saved),
                    Some(&ProviderSnapshotRevision::from_provider(&saved)),
                    vec!["m".into()],
                    BTreeMap::new(),
                    meta,
                )
                .unwrap()
                .is_some()
        );
        drop(store);
        let reopened = Store::open(temp.path().join("data")).unwrap();
        let persisted = reopened.provider(&saved.id).unwrap();
        let meta = &persisted.models_dev_meta["m"];
        assert_eq!(meta.name.as_deref(), Some("Model M"));
        assert_eq!(
            meta.supported_reasoning_levels.as_ref().unwrap(),
            &["low", "max"]
        );
        assert_eq!(meta.default_reasoning_level.as_deref(), Some("max"));
        let catalog = crate::model_unlock::build_model_catalog_with_windows_for_provider(
            &persisted,
            temp.path(),
        );
        let json = serde_json::to_value(&catalog).unwrap();
        assert_eq!(json[0]["supported_reasoning_levels"][1]["effort"], "max");
        assert_eq!(
            json[0]["supported_reasoning_levels"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(json[0]["default_reasoning_level"], "max");
    }

    #[test]
    fn activation_never_falls_back_to_cached_models_after_refresh_failure() {
        let cached = provider("cached", "https://provider.example.test/v1", "cached-model");
        let error = validate_fresh_activation_models(
            Some(AppError::Internal("network down".into())),
            &cached,
        )
        .unwrap_err();

        assert!(error.to_string().contains("连接未切换"));
        assert!(error.to_string().contains("network down"));
    }

    #[test]
    fn activation_requires_a_non_empty_fresh_model_list() {
        let blank = provider("blank", "https://provider.example.test/v1", "  ");
        let error = validate_fresh_activation_models(None, &blank).unwrap_err();
        assert!(error.to_string().contains("没有可用模型"));
        let fresh = provider("fresh", "https://provider.example.test/v1", "fresh-model");
        validate_fresh_activation_models(None, &fresh).unwrap();
    }

    #[test]
    fn custom_only_provider_can_activate_when_models_endpoint_is_unavailable() {
        let mut custom = provider("custom", "https://provider.example.test/v1", "");
        custom.available_models.clear();
        custom.custom_models = vec!["manual-model".into()];
        validate_fresh_activation_models(
            Some(AppError::Internal("models endpoint unavailable".into())),
            &custom,
        )
        .unwrap();

        custom.available_models = vec!["stale-api-model".into()];
        custom.selected_models = Some(vec!["manual-model".into()]);
        validate_fresh_activation_models(
            Some(AppError::Internal("models endpoint unavailable".into())),
            &custom,
        )
        .unwrap();

        custom.selected_models = Some(Vec::new());
        assert!(
            validate_fresh_activation_models(
                Some(AppError::Internal("models endpoint unavailable".into())),
                &custom,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn active_provider_all_models_filter_resyncs_without_switching() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();

        let mut initial = provider("provider", "https://provider.example.test/v1", "model-a");
        initial.available_models = vec!["model-a".into(), "model-b".into()];
        initial.selected_models = Some(vec!["model-a".into()]);
        let saved = store.connections_save_provider(initial).unwrap();
        store.activate(&saved.id).unwrap();
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();

        let catalog_path = home
            .join(crate::model_unlock::MODEL_CATALOG_DIR)
            .join(crate::model_unlock::MODEL_CATALOG_FILE);
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
        let slugs = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|model| model["slug"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(slugs, vec!["model-a"]);

        // 保存当前 Provider 的“全选”后直接同步，不切换 active Provider。
        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.selected_models = None;
        store.connections_save_provider(edited).unwrap();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();

        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(catalog_path).unwrap()).unwrap();
        let slugs = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|model| model["slug"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(slugs, vec!["model-a", "model-b"]);
        assert!(store.is_active_provider("provider").unwrap());
    }

    #[tokio::test]
    async fn active_provider_context_override_resyncs_catalog_and_clears_to_automatic() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let mut initial = provider("provider", "https://provider.example.test/v1", "model-a");
        initial.available_models = vec!["model-a".into(), "model-b".into()];
        initial.model_context_windows =
            BTreeMap::from([("model-a".into(), 128_000), ("model-b".into(), 256_000)]);
        initial.context_window_override = Some(1_000_000);
        let saved = store.connections_save_provider(initial).unwrap();
        store.activate(&saved.id).unwrap();
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();

        let catalog_path = home
            .join(crate::model_unlock::MODEL_CATALOG_DIR)
            .join(crate::model_unlock::MODEL_CATALOG_FILE);
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
        for model in catalog["models"].as_array().unwrap() {
            assert_eq!(model["context_window"], 1_000_000);
            assert_eq!(model["max_context_window"], 1_000_000);
        }

        let mut cleared = store.provider(&saved.id).unwrap();
        cleared.context_window_override = None;
        store.connections_save_provider(cleared).unwrap();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(catalog_path).unwrap()).unwrap();
        let windows = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| {
                (
                    model["slug"].as_str().unwrap(),
                    model["context_window"].as_u64(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(windows["model-a"], Some(128_000));
        assert_eq!(windows["model-b"], Some(256_000));
        assert!(store.is_active_provider("provider").unwrap());
    }

    #[tokio::test]
    async fn active_provider_failure_restores_the_exact_previous_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let original = store
            .connections_save_provider(provider(
                "provider",
                "https://old.example.test/v1",
                "old-model",
            ))
            .unwrap();
        store.activate(&original.id).unwrap();
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();
        let previous = store.read(|state| state.providers[0].clone()).unwrap();

        let changed = store
            .connections_save_provider(provider(
                "provider",
                "https://changed.example.test/v1",
                "ignored-incoming-model",
            ))
            .unwrap();
        let changed_revision = ProviderSnapshotRevision::from_provider(&changed);
        let error = restore_active_provider_save(
            &store,
            &manager,
            &proxy,
            &changed_revision,
            &previous,
            AppError::Internal("配置同步失败".into()),
        )
        .await;

        assert!(error.to_string().contains("配置同步失败"));
        let restored = store.read(|state| state.providers[0].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(restored).unwrap(),
            serde_json::to_value(previous).unwrap()
        );
        let config = std::fs::read_to_string(home.join("config.toml")).unwrap();
        assert!(config.contains("https://old.example.test/v1"));
        assert!(config.contains("model = \"old-model\""));
    }

    #[tokio::test]
    async fn direct_provider_apply_records_the_effective_api_model() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let provider = provider("provider", "https://provider.example.test/v1", "api-model");
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        let preview = manager
            .preview_custom(&home, &provider, &direct_target(&provider.base_url))
            .unwrap();

        apply_provider_configuration_and_record(
            &store,
            &manager,
            &proxy,
            &home,
            &preview.operation_id,
        )
        .await
        .unwrap();

        assert_eq!(
            store.last_managed_model().unwrap().as_deref(),
            Some("api-model")
        );
    }

    #[tokio::test]
    async fn direct_provider_apply_failure_compensates_to_the_active_connection() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let original = store
            .connections_save_provider(provider(
                "original",
                "https://original.example.test/v1",
                "original-model",
            ))
            .unwrap();
        store.activate(&original.id).unwrap();
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();

        let replacement = provider(
            "replacement",
            "https://replacement.example.test/v1",
            "replacement-model",
        );
        let preview = manager
            .preview_custom(&home, &replacement, &direct_target(&replacement.base_url))
            .unwrap();
        // 使 operation 过期，模拟 apply 在提交前后任一阶段报告失败；补偿路径
        // 必须以仍处于 active 的 original Provider 重建配置。
        std::fs::write(home.join("config.toml"), "model = \"external-change\"\n").unwrap();

        let error = apply_provider_configuration_and_record(
            &store,
            &manager,
            &proxy,
            &home,
            &preview.operation_id,
        )
        .await
        .err()
        .expect("concurrent config change must reject the apply");

        assert!(matches!(error, AppError::StaleOperation));
        let config = std::fs::read_to_string(home.join("config.toml")).unwrap();
        assert!(config.contains("https://original.example.test/v1"));
        assert!(config.contains("model = \"original-model\""));
    }

    #[tokio::test]
    async fn first_activation_store_failure_restores_all_codex_files() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let catalog_path = home
            .join(crate::model_unlock::MODEL_CATALOG_DIR)
            .join(crate::model_unlock::MODEL_CATALOG_FILE);
        std::fs::create_dir_all(catalog_path.parent().unwrap()).unwrap();
        let original_config = b"model = \"user-model\"\nuser_setting = true\n";
        let original_auth = br#"{"auth_mode":"chatgpt","token":"official-secret"}"#;
        let original_catalog = br#"{"models":[{"slug":"user-model"}]}"#;
        std::fs::write(home.join("config.toml"), original_config).unwrap();
        std::fs::write(home.join("auth.json"), original_auth).unwrap();
        std::fs::write(&catalog_path, original_catalog).unwrap();

        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                state.codex.last_managed_model = Some("previous-managed".into());
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            store.snapshot().unwrap().active.kind,
            ActiveKind::None
        ));

        let provider = provider(
            "candidate",
            "https://candidate.example.test/v1",
            "candidate-model",
        );
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        let preview = manager
            .preview_custom(&home, &provider, &direct_target(&provider.base_url))
            .unwrap();
        let rollback = apply_provider_configuration_and_record(
            &store,
            &manager,
            &proxy,
            &home,
            &preview.operation_id,
        )
        .await
        .unwrap();

        let store_error = store.activate("missing-provider").unwrap_err();
        let error =
            rollback_provider_activation(&store, &manager, &proxy, rollback, store_error).await;

        assert!(error.to_string().contains("不存在"));
        assert_eq!(
            std::fs::read(home.join("config.toml")).unwrap(),
            original_config
        );
        assert_eq!(
            std::fs::read(home.join("auth.json")).unwrap(),
            original_auth
        );
        assert_eq!(std::fs::read(catalog_path).unwrap(), original_catalog);
        assert_eq!(
            store.last_managed_model().unwrap().as_deref(),
            Some("previous-managed")
        );
        assert!(matches!(
            store.snapshot().unwrap().active.kind,
            ActiveKind::None
        ));
    }

    #[tokio::test]
    async fn active_model_refresh_config_failure_restores_previous_cache() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let saved = store
            .connections_save_provider(provider(
                "provider",
                "https://provider.example.test/v1",
                "old-model",
            ))
            .unwrap();
        store.activate(&saved.id).unwrap();
        let manager = ConfigManager::default();
        let proxy = ChatProxyRegistry::default();
        sync_active_codex_configuration(&store, &manager, &proxy)
            .await
            .unwrap();
        let (previous, starting_revision, was_active) =
            store.provider_model_refresh_snapshot(&saved.id).unwrap();
        assert!(was_active);
        let source = ProviderSourceFingerprint::from_provider(&previous);
        let refreshed_revision = store
            .update_provider_models_if_source_matches(
                &saved.id,
                &source,
                Some(&starting_revision),
                vec!["new-model".into()],
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap()
            .unwrap();

        // 模拟刷新提交缓存后，Codex 配置在最终同步阶段无法解析。
        std::fs::write(home.join("config.toml"), "model = [\n").unwrap();
        let error = sync_active_provider_configuration(
            &store,
            &manager,
            &ActivationLock::default(),
            &proxy,
            &saved.id,
            &previous,
            &refreshed_revision,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("配置"));
        assert_eq!(
            store.provider(&saved.id).unwrap().available_models,
            vec!["old-model"]
        );
    }

    #[tokio::test]
    async fn manual_and_background_refresh_recover_retired_and_legacy_empty_selections() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"data":[{"id":"new-zeta"},{"id":"new-alpha"}]}))
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .resolve("models.dev", address)
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap();

        for (manual, legacy_empty) in [(false, false), (false, true), (true, false), (true, true)] {
            let temp = tempfile::tempdir().unwrap();
            let home = temp.path().join("codex-home");
            std::fs::create_dir_all(&home).unwrap();
            let store = Store::open(temp.path().join("data")).unwrap();
            store
                .update(|state| {
                    state.codex.home = home.display().to_string();
                    Ok(())
                })
                .unwrap();
            let mut initial = provider("provider", &format!("http://{address}/v1"), "old-model");
            initial.selected_models = Some(vec!["old-model".into()]);
            let saved = store.connections_save_provider(initial).unwrap();
            store.activate(&saved.id).unwrap();
            let manager = ConfigManager::default();
            let proxy = ChatProxyRegistry::default();
            sync_active_codex_configuration(&store, &manager, &proxy)
                .await
                .unwrap();
            if legacy_empty {
                // Older versions could persist this selection even though the
                // installed TOML still points at the previously selected model.
                store
                    .update(|state| {
                        state.providers[0].selected_models = Some(Vec::new());
                        Ok(())
                    })
                    .unwrap();
            }
            drop(store);
            let store = Store::open(temp.path().join("data")).unwrap();

            let models = refresh_active_models(
                &store,
                || Ok(client.clone()),
                &ActivationLock::default(),
                &proxy,
                manual.then_some(&manager),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(models, ["new-alpha", "new-zeta"]);
            let reopened = Store::open(temp.path().join("data")).unwrap();
            let refreshed = reopened.provider(&saved.id).unwrap();
            assert_eq!(refreshed.available_models, models);
            assert_eq!(refreshed.selected_models, Some(vec!["new-alpha".into()]));
            assert_eq!(
                reopened.last_managed_model().unwrap().as_deref(),
                Some("new-alpha")
            );
            let config = std::fs::read_to_string(home.join("config.toml")).unwrap();
            assert!(config.contains(r#"model = "new-alpha""#), "{config}");
            let catalog: serde_json::Value = serde_json::from_slice(
                &std::fs::read(home.join("model-catalogs/codex-tools.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(catalog["models"].as_array().unwrap().len(), 1);
            assert_eq!(catalog["models"][0]["slug"], "new-alpha");
        }
        server.abort();
    }

    #[tokio::test]
    async fn refresh_started_inactive_never_rolls_back_after_later_activation() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let saved = store
            .connections_save_provider(provider(
                "provider",
                "https://provider.example.test/v1",
                "old-model",
            ))
            .unwrap();
        let (previous, _, was_active) = store.provider_model_refresh_snapshot(&saved.id).unwrap();
        assert!(!was_active);
        let source = ProviderSourceFingerprint::from_provider(&previous);
        let refreshed_revision = store
            .update_provider_models_if_source_matches(
                &saved.id,
                &source,
                None,
                vec!["new-model".into()],
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap()
            .unwrap();
        store.activate(&saved.id).unwrap();
        std::fs::write(home.join("config.toml"), "model = [\n").unwrap();

        finish_provider_model_refresh(
            &store,
            &ConfigManager::default(),
            &ActivationLock::default(),
            &ChatProxyRegistry::default(),
            was_active,
            &previous,
            &refreshed_revision,
        )
        .await
        .unwrap();

        assert_eq!(
            store.provider(&saved.id).unwrap().available_models,
            vec!["new-model"]
        );
        assert_eq!(
            std::fs::read_to_string(home.join("config.toml")).unwrap(),
            "model = [\n"
        );
    }

    #[tokio::test]
    async fn stale_active_save_fetch_never_rolls_back_a_newer_source() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let previous = store
            .connections_save_provider(provider(
                "provider",
                "https://old.example.test/v1",
                "old-model",
            ))
            .unwrap();
        store.activate(&previous.id).unwrap();

        let first_save = store
            .connections_save_provider(provider(
                "provider",
                "https://first.example.test/v1",
                "ignored",
            ))
            .unwrap();
        let first_revision = ProviderSnapshotRevision::from_provider(&first_save);
        let newer = store
            .connections_save_provider(provider(
                "provider",
                "https://newer.example.test/v1",
                "ignored",
            ))
            .unwrap();

        let error = rollback_active_save_after_model_failure(
            &store,
            &ConfigManager::default(),
            &ChatProxyRegistry::default(),
            &first_revision,
            &previous,
            AppError::StaleOperation,
        )
        .await;

        assert!(matches!(error, AppError::StaleOperation));
        let current = store.provider(&previous.id).unwrap();
        assert_eq!(current.base_url, newer.base_url);
        assert_ne!(current.base_url, previous.base_url);
    }

    #[tokio::test]
    async fn active_save_model_commit_rejects_a_newer_same_source_edit() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let previous = store
            .connections_save_provider(provider(
                "provider",
                "https://old.example.test/v1",
                "old-model",
            ))
            .unwrap();
        store.activate(&previous.id).unwrap();

        let first_save = store
            .connections_save_provider(provider(
                "provider",
                "https://new.example.test/v1",
                "ignored",
            ))
            .unwrap();
        let first_revision = ProviderSnapshotRevision::from_provider(&first_save);
        let first_source = ProviderSourceFingerprint::from_provider(&first_save);
        let mut later_edit = first_save.clone();
        later_edit.timeout_secs = 91;
        let later_edit = store.connections_save_provider(later_edit).unwrap();

        let committed = store
            .update_provider_models_if_source_matches(
                &first_save.id,
                &first_source,
                Some(&first_revision),
                vec!["late-model".into()],
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap();
        assert!(committed.is_none());

        let error = rollback_active_save_after_model_failure(
            &store,
            &ConfigManager::default(),
            &ChatProxyRegistry::default(),
            &first_revision,
            &previous,
            AppError::StaleOperation,
        )
        .await;
        assert!(matches!(error, AppError::StaleOperation));
        let current = store.provider(&previous.id).unwrap();
        assert_eq!(current.base_url, later_edit.base_url);
        assert_eq!(current.timeout_secs, 91);
        assert!(current.available_models.is_empty());
    }

    #[tokio::test]
    async fn activation_preview_rereads_provider_and_files_after_transaction_wait() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(
            home.join("config.toml"),
            "model = \"old-file-model\"\nuser_setting = \"old-files\"\n",
        )
        .unwrap();
        let store = std::sync::Arc::new(Store::open(temp.path().join("data")).unwrap());
        store
            .update(|state| {
                state.codex.home = home.display().to_string();
                Ok(())
            })
            .unwrap();
        let saved = store
            .connections_save_provider(provider(
                "provider",
                "https://old.example.test/v1",
                "old-model",
            ))
            .unwrap();
        store.activate(&saved.id).unwrap();

        let manager = std::sync::Arc::new(ConfigManager::default());
        let activation = std::sync::Arc::new(ActivationLock::default());
        let proxy = std::sync::Arc::new(ChatProxyRegistry::default());
        let model_transaction = activation.2.lock().await;
        let activation_guard = activation.0.lock().await;
        let waiting_store = store.clone();
        let waiting_manager = manager.clone();
        let waiting_activation = activation.clone();
        let waiting_proxy = proxy.clone();
        let preview = tokio::spawn(async move {
            preview_activation_in_transaction(
                &waiting_store,
                &waiting_manager,
                &waiting_activation,
                &waiting_proxy,
            )
            .await
        });
        tokio::task::yield_now().await;

        // 模拟先持锁的保存事务同时提交新 Provider 与新 Codex 文件。
        store
            .update(|state| {
                let provider = state
                    .providers
                    .iter_mut()
                    .find(|provider| provider.id == saved.id)
                    .unwrap();
                provider.base_url = "https://new.example.test/v1".into();
                provider.available_models = vec!["new-model".into()];
                provider.model_context_windows.clear();
                provider.models_dev_meta.clear();
                Ok(())
            })
            .unwrap();
        std::fs::write(
            home.join("config.toml"),
            "model = \"new-model\"\nuser_setting = \"new-files\"\n",
        )
        .unwrap();
        drop(activation_guard);
        drop(model_transaction);

        let preview = preview.await.unwrap().unwrap();
        let (_, current) = store.active_provider_and_home().unwrap();
        manager
            .check_preview_provider(&preview.operation_id, &home, &current.unwrap())
            .unwrap();
        assert!(
            manager
                .check_preview_provider(&preview.operation_id, &home, &saved)
                .is_err()
        );
        manager.apply(&preview.operation_id).unwrap();
        let written = std::fs::read_to_string(home.join("config.toml")).unwrap();
        assert!(written.contains("https://new.example.test/v1"));
        assert!(written.contains("model = \"new-model\""));
        assert!(written.contains("user_setting = \"new-files\""));
        assert!(!written.contains("https://old.example.test/v1"));
    }
}
