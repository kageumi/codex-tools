use crate::{
    codex, local_usage::UsageLedger, models::*, official_pricing, record_current_activation,
    state::ApiClient, storage::Store,
};
use chrono::TimeZone;
use tauri::State;

#[tauri::command]
#[tracing::instrument(name = "usage_get_overview", skip_all)]
pub(crate) async fn usage_get_overview(
    ledger: State<'_, UsageLedger>,
    query: UsageQuery,
) -> Result<UsageOverview, AppError> {
    let ledger = ledger.inner().clone();
    let result = tokio::task::spawn_blocking(move || ledger.query(query))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?;
    match &result {
        Ok(overview) => tracing::info!(
            operation = "usage_get_overview",
            outcome = "ok",
            row_count = overview.rows.len(),
            warning_count = overview.warnings.len()
        ),
        Err(_) => tracing::warn!(operation = "usage_get_overview", outcome = "error"),
    }
    result
}

#[tauri::command]
#[tracing::instrument(name = "usage_refresh", skip_all)]
pub(crate) async fn usage_refresh(
    store: State<'_, Store>,
    ledger: State<'_, UsageLedger>,
    query: UsageQuery,
) -> Result<UsageOverview, AppError> {
    // 对账必须发生在增量解析之前，确保本次新增事件可以使用当前 Provider/账号。
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
    let now_utc_ms = chrono::Utc::now().timestamp_millis();
    let ledger = ledger.inner().clone();
    let result = tokio::task::spawn_blocking(move || {
        let refreshed = ledger.refresh(&codex_home, now_utc_ms)?;
        let mut overview = ledger.query(query)?;
        overview.warnings = refreshed.warnings;
        if refreshed.partial_lines > 0 {
            overview.warnings.push(UsageWarning {
                path: None,
                message: format!(
                    "有 {} 条日志行尚未写完，将在下一次刷新时继续读取。",
                    refreshed.partial_lines
                ),
            });
        }
        Ok(overview)
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))?;
    match &result {
        Ok(overview) => tracing::info!(
            operation = "usage_refresh",
            outcome = "ok",
            row_count = overview.rows.len(),
            warning_count = overview.warnings.len()
        ),
        Err(_) => tracing::warn!(operation = "usage_refresh", outcome = "error"),
    }
    result
}

#[tauri::command]
pub(crate) async fn usage_get_trend(
    ledger: State<'_, UsageLedger>,
    range: UsageRange,
) -> Result<UsageTrend, AppError> {
    let ledger = ledger.inner().clone();
    tokio::task::spawn_blocking(move || ledger.trend(range))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?
}

#[tauri::command]
pub(crate) async fn usage_get_official_pricing(
    ledger: State<'_, UsageLedger>,
) -> Result<OfficialPricingCatalogView, AppError> {
    let ledger = ledger.inner().clone();
    let catalog = tokio::task::spawn_blocking(move || ledger.official_pricing_catalog())
        .await
        .map_err(|error| AppError::Internal(error.to_string()))??;
    catalog_view(catalog)
}

#[tauri::command]
pub(crate) async fn usage_refresh_official_pricing(
    client: State<'_, ApiClient>,
    ledger: State<'_, UsageLedger>,
) -> Result<OfficialPricingCatalogView, AppError> {
    let cached = ledger.official_pricing_catalog()?;
    let url = reqwest::Url::parse(official_pricing::SOURCE_URL)
        .map_err(|error| AppError::Internal(format!("官方价格地址无效：{error}")))?;
    if url.host_str() != Some("developers.openai.com") || url.scheme() != "https" {
        return Err(AppError::Internal("官方价格地址域名校验失败。".into()));
    }
    let http = client.current()?;
    let mut request = http.get(url).timeout(std::time::Duration::from_secs(8));
    if let Some(cached) = cached.as_ref() {
        if let Some(etag) = cached.etag.as_deref() {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = cached.last_modified.as_deref() {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
        }
    }
    let response = request.send().await.map_err(|error| {
        AppError::Internal(format!("获取官方价格失败，已保留上次缓存：{error}"))
    })?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return catalog_view(cached);
    }
    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "官方价格服务返回 HTTP {}，已保留上次缓存。",
            response.status()
        )));
    }
    if response
        .content_length()
        .is_some_and(|size| size as usize > official_pricing::MAX_DOCUMENT_BYTES)
    {
        return Err(AppError::Internal(
            "官方价格文档超过 512KB 限制，已保留上次缓存。".into(),
        ));
    }
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let last_modified = response
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        let chunk =
            chunk.map_err(|error| AppError::Internal(format!("读取官方价格失败：{error}")))?;
        if bytes.len().saturating_add(chunk.len()) > official_pricing::MAX_DOCUMENT_BYTES {
            return Err(AppError::Internal(
                "官方价格文档超过 512KB 限制，已保留上次缓存。".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let document = String::from_utf8(bytes)
        .map_err(|_| AppError::Internal("官方价格文档不是 UTF-8。".into()))?;
    let now = chrono::Utc::now().timestamp_millis();
    let catalog = official_pricing::build_catalog(&document, now, etag, last_modified)
        .map_err(AppError::Internal)?;
    let ledger = ledger.inner().clone();
    let catalog = tokio::task::spawn_blocking(move || {
        ledger.save_official_pricing_catalog(&catalog, now)?;
        ledger.reprice_current_cycle(now)?;
        Ok::<_, AppError>(catalog)
    })
    .await
    .map_err(|error| AppError::Internal(error.to_string()))??;
    catalog_view(Some(catalog))
}

fn catalog_view(
    catalog: Option<official_pricing::OfficialPricingCatalog>,
) -> Result<OfficialPricingCatalogView, AppError> {
    Ok(match catalog {
        Some(catalog) => OfficialPricingCatalogView {
            status: "cached".into(),
            source_url: catalog.source_url,
            version: Some(catalog.version),
            content_sha256: Some(catalog.content_sha256),
            fetched_at_ms: Some(catalog.fetched_at_ms),
            etag: catalog.etag,
            model_count: catalog.models.len(),
            rates: catalog
                .models
                .into_values()
                .map(|rate| OfficialModelRateView {
                    model: rate.model,
                    long_context_threshold: rate.long_context_threshold,
                    short: token_rates_view(rate.short),
                    long: rate.long.map(token_rates_view),
                })
                .collect(),
            models: Vec::new(),
        },
        None => OfficialPricingCatalogView {
            status: "waiting".into(),
            source_url: official_pricing::SOURCE_URL.into(),
            version: None,
            content_sha256: None,
            fetched_at_ms: None,
            etag: None,
            model_count: 0,
            models: Vec::new(),
            rates: Vec::new(),
        },
    })
}

fn token_rates_view(rates: official_pricing::TokenRates) -> TokenRatesView {
    TokenRatesView {
        input: rates.input,
        cached_input: rates.cached_input,
        cache_write: rates.cache_write,
        output: rates.output,
    }
}

#[tauri::command]
pub(crate) async fn usage_list_pricing_rules(
    ledger: State<'_, UsageLedger>,
    scope: Option<PricingScope>,
) -> Result<Vec<PricingRule>, AppError> {
    let ledger = ledger.inner().clone();
    tokio::task::spawn_blocking(move || ledger.usage_list_pricing_rules(scope))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?
}

#[tauri::command]
pub(crate) async fn usage_save_pricing_rule(
    ledger: State<'_, UsageLedger>,
    input: SavePricingRule,
) -> Result<PricingRule, AppError> {
    let ledger = ledger.inner().clone();
    tokio::task::spawn_blocking(move || ledger.usage_save_pricing_rule(input))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?
}

#[tauri::command]
pub(crate) async fn usage_delete_pricing_rule(
    ledger: State<'_, UsageLedger>,
    id: String,
) -> Result<(), AppError> {
    let ledger = ledger.inner().clone();
    tokio::task::spawn_blocking(move || ledger.usage_delete_pricing_rule(&id))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?
}

#[tauri::command]
pub(crate) async fn usage_reprice(
    ledger: State<'_, UsageLedger>,
    range: UsageRange,
) -> Result<RepriceResult, AppError> {
    let ledger = ledger.inner().clone();
    tokio::task::spawn_blocking(move || ledger.reprice(range))
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?
}

pub(crate) fn local_today_range() -> UsageRange {
    let now = chrono::Local::now();
    let start_naive = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("本地日期的午夜时间应有效");
    let end_naive = start_naive + chrono::Duration::days(1);
    let start = chrono::Local
        .from_local_datetime(&start_naive)
        .single()
        .unwrap_or(now)
        .timestamp_millis();
    let end = chrono::Local
        .from_local_datetime(&end_naive)
        .single()
        .unwrap_or_else(|| now + chrono::Duration::days(1))
        .timestamp_millis();
    UsageRange {
        start_at_ms: start,
        end_at_ms: end,
    }
}
