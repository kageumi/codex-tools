use crate::usage_log::TokenUsage;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) const SOURCE_URL: &str = "https://developers.openai.com/api/docs/pricing.md";
pub(crate) const MAX_DOCUMENT_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OfficialPricingCatalog {
    pub version: i64,
    pub content_sha256: String,
    pub source_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub fetched_at_ms: i64,
    pub models: BTreeMap<String, OfficialModelRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OfficialModelRate {
    pub model: String,
    pub short: TokenRates,
    pub long: Option<TokenRates>,
    pub long_context_threshold: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TokenRates {
    pub input: Option<i64>,
    pub cached_input: Option<i64>,
    pub cache_write: Option<i64>,
    pub output: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedCatalog {
    pub models: BTreeMap<String, OfficialModelRate>,
    pub content_sha256: String,
}

pub(crate) fn parse_markdown(document: &str) -> Result<ParsedCatalog, String> {
    let mut tables = Vec::<Vec<Vec<String>>>::new();
    let mut table = None::<Vec<Vec<String>>>;
    let mut row = None::<Vec<String>>;
    let mut cell = None::<String>;
    let parser = Parser::new_ext(document, Options::ENABLE_TABLES);

    for event in parser {
        match event {
            Event::Start(Tag::Table(_)) => table = Some(Vec::new()),
            Event::Start(Tag::TableRow) | Event::Start(Tag::TableHead) => row = Some(Vec::new()),
            Event::Start(Tag::TableCell) => cell = Some(String::new()),
            Event::Text(value) | Event::Code(value) => {
                if let Some(cell) = cell.as_mut() {
                    cell.push_str(&value);
                }
            }
            Event::End(TagEnd::TableCell) => {
                if let (Some(row), Some(cell)) = (row.as_mut(), cell.take()) {
                    row.push(cell.trim().to_owned());
                }
            }
            Event::End(TagEnd::TableRow) | Event::End(TagEnd::TableHead) => {
                if let (Some(table), Some(row)) = (table.as_mut(), row.take()) {
                    table.push(row);
                }
            }
            Event::End(TagEnd::Table) => {
                if let Some(table) = table.take() {
                    tables.push(table);
                }
            }
            _ => {}
        }
    }

    let Some(table) = tables.into_iter().find(|table| is_standard_table(table)) else {
        return Err("官方价格文档中没有找到 Standard pricing data 表。".into());
    };
    let header = &table[0];
    let index = |name: &str| {
        header
            .iter()
            .position(|value| value.eq_ignore_ascii_case(name))
    };
    let model_index = index("Model").ok_or("官方价格表缺少 Model 列。")?;
    let short_input = index("Short context input").ok_or("官方价格表缺少输入价格列。")?;
    let short_cached = index("Short context cached input");
    let short_write = index("Short context cache writes");
    let short_output = index("Short context output").ok_or("官方价格表缺少输出价格列。")?;
    let long_input = index("Long context input");
    let long_cached = index("Long context cached input");
    let long_write = index("Long context cache writes");
    let long_output = index("Long context output");

    let mut models = BTreeMap::new();
    for values in table.iter().skip(1) {
        let Some(model) = values.get(model_index).map(|value| normalize_model(value)) else {
            continue;
        };
        if model.is_empty() || model == "model" {
            continue;
        }
        let short = TokenRates {
            input: parse_price(values.get(short_input))?,
            cached_input: parse_optional_column(values, short_cached)?,
            cache_write: parse_optional_column(values, short_write)?,
            output: parse_price(values.get(short_output))?,
        };
        let long = match long_input {
            Some(long_input) if values.get(long_input).is_some_and(|value| !is_dash(value)) => {
                Some(TokenRates {
                    input: parse_price(values.get(long_input))?,
                    cached_input: parse_optional_column(values, long_cached)?,
                    cache_write: parse_optional_column(values, long_write)?,
                    output: parse_optional_column(values, long_output)?,
                })
            }
            _ => None,
        };
        models.insert(
            model.clone(),
            OfficialModelRate {
                model,
                short,
                long,
                long_context_threshold: Some(272_000),
            },
        );
    }
    if models.is_empty() {
        return Err("官方价格表没有有效模型。".into());
    }
    Ok(ParsedCatalog {
        models,
        content_sha256: sha256(document.as_bytes()),
    })
}

pub(crate) fn build_catalog(
    document: &str,
    fetched_at_ms: i64,
    etag: Option<String>,
    last_modified: Option<String>,
) -> Result<OfficialPricingCatalog, String> {
    let parsed = parse_markdown(document)?;
    Ok(OfficialPricingCatalog {
        version: fetched_at_ms,
        content_sha256: parsed.content_sha256,
        source_url: SOURCE_URL.into(),
        etag,
        last_modified,
        fetched_at_ms,
        models: parsed.models,
    })
}

pub(crate) fn resolve_model<'a>(
    catalog: &'a OfficialPricingCatalog,
    model: &str,
) -> Option<&'a OfficialModelRate> {
    let normalized = normalize_model(model);
    catalog.models.get(&normalized).or_else(|| {
        catalog
            .models
            .iter()
            .filter(|(candidate, _)| {
                normalized.starts_with(candidate.as_str())
                    && normalized.as_bytes().get(candidate.len()) == Some(&b'-')
            })
            .max_by_key(|(candidate, _)| candidate.len())
            .map(|(_, rate)| rate)
    })
}

#[cfg(test)]
pub(crate) fn calculate(
    catalog: &OfficialPricingCatalog,
    usage: &TokenUsage,
    model: &str,
) -> Result<i64, String> {
    let rate =
        resolve_model(catalog, model).ok_or_else(|| "官方价格目录暂未收录此模型。".to_owned())?;
    calculate_with_rate(rate, usage)
}

pub(crate) fn calculate_with_rate(
    rate: &OfficialModelRate,
    usage: &TokenUsage,
) -> Result<i64, String> {
    let rates = if rate
        .long_context_threshold
        .is_some_and(|threshold| usage.input_tokens > threshold)
    {
        rate.long.as_ref().unwrap_or(&rate.short)
    } else {
        &rate.short
    };
    let input = rates.input.ok_or("官方价格缺少输入价格。")?;
    let cached = usage.cached_input_tokens;
    let cache_write = usage.cache_write_input_tokens;
    let normal_input =
        usage
            .input_tokens
            .saturating_sub(cached)
            .saturating_sub(if rates.cache_write.is_some() {
                cache_write
            } else {
                0
            });
    let mut total = cost(normal_input, Some(input))?;
    total = total
        .checked_add(cost(cached, rates.cached_input)?)
        .ok_or("官方价格计算超出整数范围。")?;
    if rates.cache_write.is_some() {
        total = total
            .checked_add(cost(cache_write, rates.cache_write)?)
            .ok_or("官方价格计算超出整数范围。")?;
    }
    total = total
        .checked_add(cost(usage.output_tokens, rates.output)?)
        .ok_or("官方价格计算超出整数范围。")?;
    Ok(total)
}

fn cost(tokens: u64, price: Option<i64>) -> Result<i64, String> {
    if tokens == 0 {
        return Ok(0);
    }
    let price = price.ok_or("官方价格目录缺少必要价格。")?;
    if price < 0 {
        return Err("官方价格目录包含负价格。".into());
    }
    i64::try_from((tokens as i128) * (price as i128) / 1_000_000)
        .map_err(|_| "官方价格计算超出数据库范围。".into())
}

fn is_standard_table(table: &[Vec<String>]) -> bool {
    let Some(header) = table.first() else {
        return false;
    };
    header
        .iter()
        .any(|value| value.eq_ignore_ascii_case("Model"))
        && header
            .iter()
            .any(|value| value.eq_ignore_ascii_case("Short context input"))
        && header
            .iter()
            .any(|value| value.eq_ignore_ascii_case("Short context output"))
}

fn parse_optional_column(values: &[String], index: Option<usize>) -> Result<Option<i64>, String> {
    match index {
        Some(index) => parse_price(values.get(index)),
        None => Ok(None),
    }
}

fn parse_price(value: Option<&String>) -> Result<Option<i64>, String> {
    let Some(value) = value else { return Ok(None) };
    if is_dash(value) || value.eq_ignore_ascii_case("free") {
        return Ok(None);
    }
    let value = value.trim().trim_start_matches('$');
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() || !whole.chars().all(|char| char.is_ascii_digit()) {
        return Err(format!("官方价格格式无效：{value}"));
    }
    if fraction.len() > 6 || !fraction.chars().all(|char| char.is_ascii_digit()) {
        return Err(format!("官方价格小数位无效：{value}"));
    }
    let whole = whole.parse::<i64>().map_err(|_| "官方价格超出范围。")?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>().map_err(|_| "官方价格格式无效。")?
            * 10_i64.pow((6 - fraction.len()) as u32)
    };
    whole
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| "官方价格超出范围。".into())
        .map(Some)
}

fn is_dash(value: &str) -> bool {
    matches!(value.trim(), "-" | "—" | "–" | "")
}

fn normalize_model(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    let value = value
        .split_once('(')
        .map(|(value, _)| value)
        .unwrap_or(&value);
    value
        .strip_prefix("openai/")
        .unwrap_or(value)
        .trim()
        .to_owned()
}

fn sha256(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{build_catalog, calculate, parse_markdown, resolve_model};
    use crate::usage_log::TokenUsage;

    const FIXTURE: &str = "# Pricing\n\n### Standard pricing data\n\n| Model | Short context input | Short context cached input | Short context cache writes | Short context output | Long context input | Long context cached input | Long context cache writes | Long context output |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n| gpt-live-test-2030 | $7.77 | $0.77 | $9.99 | $17.70 | $15.54 | $1.54 | $19.98 | $35.40 |\n| gpt-no-write | $1.00 | $0.10 | - | $2.00 | - | - | - | - |\n\n### Batch pricing data\n\n| Model | Short context input | Short context output |\n| --- | --- | --- |\n| should-not-match | $0.01 | $0.02 |";

    #[test]
    fn parses_unknown_model_and_standard_table_only() {
        let catalog = build_catalog(FIXTURE, 123, None, None).unwrap();
        assert!(catalog.models.contains_key("gpt-live-test-2030"));
        assert!(!catalog.models.contains_key("should-not-match"));
    }

    #[test]
    fn calculates_dynamic_unknown_model_and_long_context() {
        let catalog = build_catalog(FIXTURE, 123, None, None).unwrap();
        let cost = calculate(
            &catalog,
            &TokenUsage {
                input_tokens: 272_001,
                cached_input_tokens: 1,
                cache_write_input_tokens: 1,
                output_tokens: 2,
                reasoning_output_tokens: 1,
                total_tokens: 272_003,
            },
            "gpt-live-test-2030",
        )
        .unwrap();
        assert_eq!(cost, 4_226_954);
        assert_eq!(
            resolve_model(&catalog, "gpt-live-test-2030-2026-01-01")
                .unwrap()
                .model,
            "gpt-live-test-2030"
        );
    }

    #[test]
    fn model_resolution_prefers_the_longest_matching_family() {
        let fixture = FIXTURE.replace(
            "| gpt-no-write |",
            "| gpt-live | $1.00 | $0.10 | - | $2.00 | - | - | - | - |\n| gpt-no-write |",
        );
        let catalog = build_catalog(&fixture, 123, None, None).unwrap();

        assert_eq!(
            resolve_model(&catalog, "gpt-live-test-2030-2026-01-01")
                .unwrap()
                .model,
            "gpt-live-test-2030"
        );
    }

    #[test]
    fn missing_cache_write_price_is_charged_as_input_once() {
        let catalog = build_catalog(FIXTURE, 123, None, None).unwrap();
        let cost = calculate(
            &catalog,
            &TokenUsage {
                input_tokens: 100,
                cached_input_tokens: 20,
                cache_write_input_tokens: 10,
                output_tokens: 10,
                reasoning_output_tokens: 0,
                total_tokens: 110,
            },
            "gpt-no-write",
        )
        .unwrap();
        assert_eq!(cost, 102);
    }

    #[test]
    fn rejects_missing_standard_table() {
        assert!(
            parse_markdown(
                "# Pricing\n\n| Model | Input | Output |\n| --- | --- | --- |\n| x | $1 | $2 |"
            )
            .is_err()
        );
    }
}
