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
    Ok(catalog_view(catalog, false))
}

#[tauri::command]
pub(crate) async fn usage_refresh_official_pricing(
    client: State<'_, ApiClient>,
    ledger: State<'_, UsageLedger>,
) -> Result<OfficialPricingCatalogView, AppError> {
    let url = reqwest::Url::parse(official_pricing::SOURCE_URL)
        .map_err(|error| AppError::Internal(format!("官方价格地址无效：{error}")))?;
    if url.host_str() != Some("developers.openai.com") || url.scheme() != "https" {
        return Err(AppError::Internal("官方价格地址域名校验失败。".into()));
    }
    refresh_pricing_from(ledger.inner().clone(), client.current()?.get(url)).await
}

async fn refresh_pricing_from(
    ledger: UsageLedger,
    request: reqwest::RequestBuilder,
) -> Result<OfficialPricingCatalogView, AppError> {
    let cached = ledger.official_pricing_catalog()?;
    let mut request = request.timeout(std::time::Duration::from_secs(8));
    let conditional = cached.as_ref().filter(|catalog| {
        catalog.parser_version == official_pricing::PARSER_VERSION
            && (catalog.etag.is_some() || catalog.last_modified.is_some())
    });
    if let Some(cached) = conditional {
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
        if conditional.is_none() {
            return Err(AppError::Internal(
                "官方价格服务未返回重新解析所需的文档，已保留上次缓存。".into(),
            ));
        }
        return sync_pricing(ledger, None, chrono::Utc::now().timestamp_millis()).await;
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
    sync_pricing(ledger, Some(catalog), now).await
}

async fn sync_pricing(
    ledger: UsageLedger,
    catalog: Option<official_pricing::OfficialPricingCatalog>,
    now: i64,
) -> Result<OfficialPricingCatalogView, AppError> {
    let (active, usage_repriced) =
        tokio::task::spawn_blocking(move || ledger.sync_official_pricing_catalog(catalog, now))
            .await
            .map_err(|error| AppError::Internal(error.to_string()))??;
    Ok(catalog_view(active, usage_repriced))
}

fn catalog_view(
    catalog: Option<official_pricing::OfficialPricingCatalog>,
    usage_repriced: bool,
) -> OfficialPricingCatalogView {
    match catalog {
        Some(catalog) => OfficialPricingCatalogView {
            usage_repriced,
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
            usage_repriced,
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
    }
}
fn token_rates_view(rates: official_pricing::TokenRates) -> TokenRatesView {
    TokenRatesView {
        input: rates.input,
        cached_input: rates.cached_input,
        cache_write: rates.cache_write,
        cache_write_12h: rates.cache_write_12h,
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

#[cfg(test)]
mod pricing_sync_tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode, header};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn stale_normalized_cache_gets_the_body_before_reusing_validators() {
        const DOC: &str = "### Standard pricing data\n| Model | Short context input | Short context 30m write | Short context 12h write | Short context output |\n| --- | --- | --- | --- | --- |\n| test-model | $1 | $2 | $5 | $8 |";
        const ETAG: &str = "\"fixture\"";
        const DATE: &str = "Thu, 01 Jan 2026 00:00:00 GMT";
        let temp = tempfile::tempdir().unwrap();
        let ledger = UsageLedger::open(temp.path()).unwrap();
        let catalog =
            official_pricing::build_catalog(DOC, 1000, Some(ETAG.into()), Some(DATE.into()))
                .unwrap();
        let hash = catalog.content_sha256.clone();
        ledger
            .sync_official_pricing_catalog(Some(catalog), 1000)
            .unwrap();
        let db = rusqlite::Connection::open(temp.path().join("usage.sqlite3")).unwrap();
        db.execute("UPDATE official_pricing_catalogs SET normalized_json=json_remove(normalized_json,
            '$.parserVersion', '$.models.test-model.short.cacheWrite', '$.models.test-model.short.cacheWrite12h')", []).unwrap();
        let now = chrono::Utc::now();
        let home = temp.path().join("home");
        ledger
            .refresh(&home, now.timestamp_millis() - 1000)
            .unwrap();
        let sessions = home.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let event = serde_json::json!({"type":"event_msg","timestamp":now.to_rfc3339(),"payload":{
            "type":"token_count","info":{"last_token_usage":{"input_tokens":10,"cache_write_input_tokens":4,"output_tokens":0,"total_tokens":10}}}});
        std::fs::write(sessions.join("rollout-pricing.jsonl"), format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"pricing\",\"model_provider\":\"openai\"}}}}\n{{\"type\":\"turn_context\",\"payload\":{{\"model\":\"test-model\"}}}}\n{event}\n")).unwrap();
        assert_eq!(
            ledger
                .refresh(&home, now.timestamp_millis())
                .unwrap()
                .events_added,
            1
        );
        let cost = || {
            db.query_row(
                "SELECT estimated_cost_microusd FROM usage_events",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        };
        assert_eq!(cost(), 10);
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let visits = Arc::new(AtomicUsize::new(0));
        let router = axum::Router::new().route(
            "/",
            axum::routing::get(move |headers: HeaderMap| {
                let index = visits.fetch_add(1, Ordering::Relaxed);
                sent.send(headers).unwrap();
                async move {
                    let (status, body) = if index == 1 {
                        (StatusCode::OK, DOC)
                    } else {
                        (StatusCode::NOT_MODIFIED, "")
                    };
                    (
                        status,
                        [(header::ETAG, ETAG), (header::LAST_MODIFIED, DATE)],
                        body,
                    )
                }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
        let http = reqwest::Client::builder().no_proxy().build().unwrap();
        let refresh = || refresh_pricing_from(ledger.clone(), http.get(&url));
        assert!(refresh().await.is_err());
        let old = ledger.official_pricing_catalog().unwrap().unwrap();
        assert_eq!(old.parser_version, 0);
        assert!(old.models["test-model"].short.cache_write.is_none());
        let fresh = refresh().await.unwrap();
        assert_eq!(fresh.content_sha256.as_deref(), Some(hash.as_str()));
        assert!(fresh.usage_repriced);
        assert_eq!(cost(), 14);
        assert_eq!(fresh.rates[0].short.cache_write, Some(2_000_000));
        assert_eq!(fresh.rates[0].short.cache_write_12h, Some(5_000_000));
        for _ in 0..2 {
            let headers = received.recv().await.unwrap();
            assert!(!headers.contains_key(header::IF_NONE_MATCH));
            assert!(!headers.contains_key(header::IF_MODIFIED_SINCE));
        }
        assert!(!refresh().await.unwrap().usage_repriced);
        let headers = received.recv().await.unwrap();
        assert_eq!(headers[header::IF_NONE_MATCH], ETAG);
        assert_eq!(headers[header::IF_MODIFIED_SINCE], DATE);
        server.abort();
    }
}
