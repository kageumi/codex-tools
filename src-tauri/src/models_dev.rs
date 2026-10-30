//! 从 models.dev 抓取模型元数据（运行时数据源，不内置快照）。
//!
//! 模型列表、上下文窗口、简介等数据随时会变化，因此本应用**不内置任何
//! 模型数据快照**。模型**列表**只来自服务商自己的 `/models` 接口；本模块
//! 负责抓取 models.dev 的 `models.json`（约 250KB，按 `服务商/模型id` 键控），
//! 为 `/models` 接口返回的模型**匹配**补充元数据（展示名、上下文窗口、简介）。
//!
//! 第三方 API 返回的模型 id 写法多样，本模块兼容：
//! - 纯模型 id：`deepseek-v4-flash`
//! - 带厂商前缀：`deepseek/deepseek-v4-flash`、`deepseek:deepseek-v4-flash`
//! - 大小写差异
//!
//! 匹配失败或数据不可用时一律丢弃，不硬编码任何模型数据。

use crate::models::{AppError, ProviderModelsDevMeta};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

pub(crate) const SOURCE_URL: &str = "https://models.dev/models.json";
/// models.dev 的 models.json 约 250KB，留出余量防止将来膨胀。
pub(crate) const MAX_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
/// models.dev 数据变化不频繁。进程内缓存该文档，避免每次同步模型
/// （前台每 10 分钟一次 + 每次保存/编辑/切换）都重新下载并解析。
const DOCUMENT_CACHE_TTL: Duration = Duration::from_secs(12 * 60 * 60);

struct DocumentCache {
    fetched_at: Instant,
    document: String,
}

static DOCUMENT_CACHE: OnceLock<Mutex<Option<DocumentCache>>> = OnceLock::new();

fn document_cache() -> &'static Mutex<Option<DocumentCache>> {
    DOCUMENT_CACHE.get_or_init(|| Mutex::new(None))
}

/// 返回未过期的缓存文档；`now` 由调用方传入，便于测试控制时间。
fn cached_document(now: Instant) -> Option<String> {
    let cache = document_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.as_ref().and_then(|entry| {
        (now.duration_since(entry.fetched_at) < DOCUMENT_CACHE_TTL).then(|| entry.document.clone())
    })
}

/// 返回任意缓存文档（含过期），供网络失败时回退。
fn stale_document() -> Option<String> {
    let cache = document_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.as_ref().map(|entry| entry.document.clone())
}

fn store_document(now: Instant, document: String) {
    let mut cache = document_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache = Some(DocumentCache {
        fetched_at: now,
        document,
    });
}

/// 抓取 models.dev 并构建模型元数据索引。
/// 索引的键包含模型 id 的常见写法（全键 / 纯 id / 小写），
/// 供 [`lookup_model`] 按 `/models` 接口返回的原始 id 解析。
/// 找不到匹配服务商时仍返回全量索引（纯 id 无跨厂商冲突）；
/// 网络或解析失败时返回错误，由调用方决定保留旧数据。
pub(crate) async fn fetch_provider_meta(
    client: &reqwest::Client,
    base_url: &str,
    provider_name: &str,
) -> Result<BTreeMap<String, ProviderModelsDevMeta>, AppError> {
    let document = fetch_document(client).await?;
    let entries = parse_document(&document)?;
    let matched = find_provider_id(&entries, base_url, provider_name);
    let mut index: BTreeMap<String, ProviderModelsDevMeta> = BTreeMap::new();
    // 第一遍：全量索引（全键 + 纯模型 id，统一小写）。纯 id 冲突时先到先得。
    for (provider_id, model_id, meta) in &entries {
        index.insert(provider_id_key(provider_id, model_id), meta.clone());
        index
            .entry(model_id.to_ascii_lowercase())
            .or_insert_with(|| meta.clone());
    }
    // 第二遍：匹配到的服务商优先覆盖纯 id 条目（gpt-4o 之类跨厂商同名时选对厂商）。
    if let Some(matched_provider) = matched {
        for (provider_id, model_id, meta) in entries
            .iter()
            .filter(|(provider_id, _, _)| *provider_id == matched_provider)
        {
            index.insert(provider_id_key(provider_id, model_id), meta.clone());
            index.insert(model_id.to_ascii_lowercase(), meta.clone());
        }
    }
    Ok(index)
}

/// 从索引中解析 `/models` 接口返回的单个模型 id，兼容多种写法。
pub(crate) fn lookup_model<'a>(
    index: &'a BTreeMap<String, ProviderModelsDevMeta>,
    id: &str,
) -> Option<&'a ProviderModelsDevMeta> {
    let id = id.trim();
    if id.is_empty() {
        return None;
    }
    let lower = id.to_ascii_lowercase();
    // 1. 全键精确（含厂商前缀，如 deepseek/deepseek-v4-flash）。
    if let Some(meta) = index.get(&lower) {
        return Some(meta);
    }
    // 2. 去掉厂商前缀后的纯 id（按 / 或 : 取最后一节）。
    let plain = id.rsplit(['/', ':']).next().unwrap_or(id);
    let plain_lower = plain.to_ascii_lowercase();
    if plain != id {
        if let Some(meta) = index.get(&plain_lower) {
            return Some(meta);
        }
    }
    // 3. 近似匹配：逐级去掉版本/日期/预览后缀，借用同族模型的参数。
    //    如 `deepseek-v4-flash-0731` → `deepseek-v4-flash`，
    //    `gpt-4o-2024-11-20` → `gpt-4o`。
    let mut candidate = plain_lower;
    while let Some(stripped) = strip_version_suffix(&candidate) {
        if let Some(meta) = index.get(&stripped) {
            return Some(meta);
        }
        candidate = stripped;
    }
    None
}

/// 去掉模型 id 末尾的版本/日期/预览后缀段（以 `-` 分隔，且小写后为纯数字
/// 或常见标记）；`70b`、`27b` 这类参数量后缀不会被误剥。
fn strip_version_suffix(id: &str) -> Option<String> {
    let (head, tail) = id.rsplit_once('-')?;
    let tail = tail.to_ascii_lowercase();
    let looks_like_version = !tail.is_empty()
        && (tail.chars().all(|char| char.is_ascii_digit())
            || matches!(
                tail.as_str(),
                "preview" | "latest" | "beta" | "snapshot" | "draft" | "dev" | "alpha" | "rc"
            ));
    if looks_like_version && !head.is_empty() {
        Some(head.to_string())
    } else {
        None
    }
}

/// 解析 `models.json`（`{ "服务商/模型id": {...} }`），返回 `(服务商id, 模型id, 元数据)`。
fn parse_document(
    document: &str,
) -> Result<Vec<(String, String, ProviderModelsDevMeta)>, AppError> {
    let payload: BTreeMap<String, RawModel> = serde_json::from_str(document)
        .map_err(|error| AppError::Internal(format!("models.dev 数据格式无效：{error}")))?;
    let mut entries = Vec::new();
    for (key, raw) in payload {
        let Some((provider_id, model_id)) = key.split_once('/') else {
            continue;
        };
        if provider_id.trim().is_empty() || model_id.trim().is_empty() {
            continue;
        }
        let context_window = raw
            .limit
            .as_ref()
            .and_then(|limit| parse_context_window(limit.context.as_ref()))
            .filter(|window| *window > 0);
        let description = raw
            .description
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned);
        let name = raw
            .name
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned);
        entries.push((
            provider_id.to_ascii_lowercase(),
            model_id.to_ascii_lowercase(),
            ProviderModelsDevMeta {
                name,
                context_window,
                description,
                supported_reasoning_levels: None,
                default_reasoning_level: None,
            },
        ));
    }
    if entries.is_empty() {
        return Err(AppError::Internal("models.dev 数据中没有可用模型。".into()));
    }
    Ok(entries)
}

fn provider_id_key(provider_id: &str, model_id: &str) -> String {
    format!("{provider_id}/{model_id}")
}

/// 按服务商名称 / `base_url` 主机匹配 models.dev 的服务商 id：
/// 1. 服务名称（去除标点、忽略大小写）与键前缀一致，如「DeepSeek」→ `deepseek`；
/// 2. `base_url` 主机包含服务商 id（如 `api.deepseek.com` 含 `deepseek`）。
fn find_provider_id(
    entries: &[(String, String, ProviderModelsDevMeta)],
    base_url: &str,
    provider_name: &str,
) -> Option<String> {
    let provider_ids = entries
        .iter()
        .map(|(provider_id, _, _)| provider_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let normalized_name = normalize_name(provider_name);
    // 名称一致优先。
    if let Some(matched) = provider_ids
        .iter()
        .find(|provider_id| normalize_name(provider_id) == normalized_name)
    {
        return Some((*matched).to_owned());
    }
    // 主机包含兜底：归一化（去 api/www 前缀和分隔符）后比对，
    // 如 `api.moonshot.ai` → `moonshotai`。
    if let Some(host) = host_of(base_url) {
        let normalized_host = host
            .trim_start_matches("api.")
            .trim_start_matches("www.")
            .chars()
            .filter(|char| char.is_ascii_alphanumeric())
            .collect::<String>();
        if let Some(matched) = provider_ids.iter().find(|provider_id| {
            let provider_id = provider_id.to_ascii_lowercase();
            normalized_host == provider_id || normalized_host.contains(&provider_id)
        }) {
            return Some((*matched).to_owned());
        }
    }
    None
}

fn host_of(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
}

fn normalize_name(value: &str) -> String {
    value
        .chars()
        .filter(|char| char.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// `limit.context` 可能是数字，也可能是 `{ "total": ... }` 形态的对象。
fn parse_context_window(value: Option<&serde_json::Value>) -> Option<u64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_u64(),
        Some(serde_json::Value::Object(map)) => {
            map.get("total").and_then(serde_json::Value::as_u64)
        }
        _ => None,
    }
}

pub(crate) async fn fetch_document(client: &reqwest::Client) -> Result<String, AppError> {
    if let Some(document) = cached_document(Instant::now()) {
        return Ok(document);
    }
    match fetch_document_uncached(client).await {
        Ok(document) => {
            store_document(Instant::now(), document.clone());
            Ok(document)
        }
        // 网络失败时回退到任意缓存（含过期），避免每次同步都重复抓取失败；
        // 调用方对 models.dev 数据本身就有“抓取失败时保留旧数据”的容错。
        Err(error) => stale_document().ok_or(error),
    }
}

async fn fetch_document_uncached(client: &reqwest::Client) -> Result<String, AppError> {
    let url = reqwest::Url::parse(SOURCE_URL)
        .map_err(|error| AppError::Internal(format!("models.dev 地址无效：{error}")))?;
    if url.host_str() != Some("models.dev") || url.scheme() != "https" {
        return Err(AppError::Internal("models.dev 地址域名校验失败。".into()));
    }
    let response = client
        .get(url)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("获取 models.dev 数据失败：{error}")))?;
    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "models.dev 返回 HTTP {}。",
            response.status()
        )));
    }
    if response
        .content_length()
        .is_some_and(|size| size as usize > MAX_DOCUMENT_BYTES)
    {
        return Err(AppError::Internal("models.dev 数据超过 4MB 限制。".into()));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        let chunk = chunk
            .map_err(|error| AppError::Internal(format!("读取 models.dev 数据失败：{error}")))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_DOCUMENT_BYTES {
            return Err(AppError::Internal("models.dev 数据超过 4MB 限制。".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|_| AppError::Internal("models.dev 数据不是 UTF-8。".into()))
}

#[derive(Deserialize)]
struct RawModel {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    limit: Option<RawLimit>,
}

#[derive(Deserialize)]
struct RawLimit {
    context: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::{
        DOCUMENT_CACHE_TTL, cached_document, find_provider_id, lookup_model, parse_document,
        stale_document, store_document,
    };
    use crate::models::ProviderModelsDevMeta;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    const FIXTURE: &str = r#"{
      "deepseek/deepseek-v4-pro": {
        "name": "DeepSeek V4 Pro",
        "description": "百万 token 上下文旗舰模型",
        "limit": { "context": 1000000, "output": 384000 }
      },
      "deepseek/deepseek-v4-flash": {
        "name": "DeepSeek V4 Flash",
        "limit": { "context": 0, "output": 0 }
      },
      "moonshotai/kimi-k2": {
        "name": "Kimi K2",
        "limit": { "context": 131072, "output": 32768 }
      },
      "openai/gpt-5.2": {
        "name": "GPT-5.2",
        "description": "通用模型",
        "limit": { "context": 400000 }
      }
    }"#;

    fn index() -> BTreeMap<String, ProviderModelsDevMeta> {
        // 直接用 fetch_provider_meta 之外的轻量方式：手动构造索引
        let entries = parse_document(FIXTURE).unwrap();
        let mut index = BTreeMap::new();
        for (pid, mid, meta) in &entries {
            index.insert(format!("{pid}/{mid}"), meta.clone());
            index.insert(mid.clone(), meta.clone());
        }
        index
    }

    #[test]
    fn parses_models_json_shape() {
        let entries = parse_document(FIXTURE).unwrap();
        assert_eq!(entries.len(), 4);
        let (pid, _mid, meta) = entries
            .iter()
            .find(|(_, mid, _)| mid == "deepseek-v4-pro")
            .unwrap();
        assert_eq!(pid, "deepseek");
        assert_eq!(meta.context_window, Some(1_000_000));
        assert_eq!(meta.name.as_deref(), Some("DeepSeek V4 Pro"));
        // context 为 0 视为未知。
        let flash = entries
            .iter()
            .find(|(_, mid, _)| mid == "deepseek-v4-flash")
            .unwrap();
        assert_eq!(flash.2.context_window, None);
    }

    #[test]
    fn lookup_handles_plain_and_prefixed_ids() {
        let index = index();
        // 纯模型 id。
        assert_eq!(
            lookup_model(&index, "deepseek-v4-pro").and_then(|meta| meta.name.as_deref()),
            Some("DeepSeek V4 Pro")
        );
        // 厂商前缀（/ 和 : 分隔符）。
        assert_eq!(
            lookup_model(&index, "deepseek/deepseek-v4-pro").and_then(|meta| meta.context_window),
            Some(1_000_000)
        );
        assert_eq!(
            lookup_model(&index, "deepseek:deepseek-v4-pro").and_then(|meta| meta.context_window),
            Some(1_000_000)
        );
        // 大小写差异。
        assert_eq!(
            lookup_model(&index, "DeepSeek/DeepSeek-V4-Pro").and_then(|meta| meta.context_window),
            Some(1_000_000)
        );
        // 不存在。
        assert!(lookup_model(&index, "no-such-model").is_none());
    }

    #[test]
    fn matches_provider_by_name_and_host() {
        let entries = parse_document(FIXTURE).unwrap();
        // 名称一致。
        assert_eq!(
            find_provider_id(&entries, "https://unknown.example/v1", "DeepSeek"),
            Some("deepseek".into())
        );
        // 主机包含（moonshotai 的主机）。
        assert_eq!(
            find_provider_id(&entries, "https://api.moonshot.ai/v1", "任意名称"),
            Some("moonshotai".into())
        );
        // 都不匹配。
        assert_eq!(
            find_provider_id(&entries, "https://unknown.example/v1", "其他"),
            None
        );
    }

    #[test]
    fn fuzzy_matches_by_stripping_version_and_date_suffixes() {
        let index = index();
        // 日期后缀 → 同族模型。
        assert_eq!(
            lookup_model(&index, "deepseek-v4-flash-0731").and_then(|meta| meta.name.as_deref()),
            Some("DeepSeek V4 Flash")
        );
        // 多段日期后缀逐级剥离。
        assert_eq!(
            lookup_model(&index, "deepseek-v4-pro-2025-01-31").and_then(|meta| meta.context_window),
            Some(1_000_000)
        );
        // preview 后缀。
        assert_eq!(
            lookup_model(&index, "kimi-k2-0711-preview").and_then(|meta| meta.name.as_deref()),
            Some("Kimi K2")
        );
        // 参数量后缀（70b / 27b）不应被误剥。
        assert!(lookup_model(&index, "llama-3.3-70b-instruct").is_none());
        assert!(lookup_model(&index, "gpt-5.2-70b").is_none());
    }

    #[test]
    fn rejects_invalid_documents() {
        assert!(parse_document("not json").is_err());
        assert!(parse_document("{}").is_err());
        assert!(parse_document(r#"{"no-slash-key": {}}"#).is_err());
    }

    #[test]
    fn document_cache_serves_fresh_expires_and_keeps_stale() {
        let now = Instant::now();
        assert!(cached_document(now).is_none());
        assert!(stale_document().is_none());

        store_document(now, "models-doc".into());
        assert_eq!(cached_document(now).as_deref(), Some("models-doc"));

        let expired = now + DOCUMENT_CACHE_TTL + Duration::from_secs(1);
        assert!(cached_document(expired).is_none());
        // 过期后仍保留旧数据，供网络失败回退。
        assert_eq!(stale_document().as_deref(), Some("models-doc"));

        // 重新写入后恢复可用。
        store_document(expired, "fresh-doc".into());
        assert_eq!(cached_document(expired).as_deref(), Some("fresh-doc"));
    }
}
