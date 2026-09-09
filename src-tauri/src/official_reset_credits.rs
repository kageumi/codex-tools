use crate::{
    models::{
        ResetCredit, ResetCreditConsumeOutcome, ResetCreditDetails, ResetCreditDetailsStatus,
        StoredOfficialAccount,
    },
    official_quota::{
        QuotaFetchError, classify_http_error, official_headers, parse_reset_credit_summary,
        parse_timestamp, read_bounded_json, read_optional_error_json,
    },
};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use serde_json::{Value, json};
use std::time::Duration;

const RESET_CREDITS_URL: &str = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits";
const RESET_CREDITS_CONSUME_URL: &str =
    "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume";

pub(crate) async fn fetch_reset_credits(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
) -> Result<ResetCreditDetails, QuotaFetchError> {
    fetch_reset_credits_from(client, account, RESET_CREDITS_URL).await
}

async fn fetch_reset_credits_from(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
    endpoint: &str,
) -> Result<ResetCreditDetails, QuotaFetchError> {
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        client
            .get(endpoint)
            .headers(official_headers(account)?)
            .send(),
    )
    .await
    .map_err(|_| QuotaFetchError::retryable("OpenAI 重置卡详情查询超时，请稍后重试"))?
    .map_err(|_| QuotaFetchError::retryable("无法连接 OpenAI 重置卡服务，请检查网络或系统代理"))?;
    if !response.status().is_success() {
        let status = response.status();
        let payload = read_optional_error_json(response).await;
        return Err(classify_http_error(status, payload.as_ref(), None));
    }
    let payload = read_bounded_json(response).await?;
    Ok(parse_reset_credit_details(&payload, &account.id))
}

pub(crate) async fn consume_reset_credit(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
    credit_id: &str,
    idempotency_key: &str,
) -> Result<ResetCreditConsumeOutcome, QuotaFetchError> {
    consume_reset_credit_at(
        client,
        account,
        credit_id,
        idempotency_key,
        RESET_CREDITS_CONSUME_URL,
    )
    .await
}

async fn consume_reset_credit_at(
    client: &reqwest::Client,
    account: &StoredOfficialAccount,
    credit_id: &str,
    idempotency_key: &str,
    endpoint: &str,
) -> Result<ResetCreditConsumeOutcome, QuotaFetchError> {
    let mut headers = official_headers(account)?;
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        client
            .post(endpoint)
            .headers(headers)
            .json(&json!({
                "redeem_request_id": idempotency_key,
                "credit_id": credit_id,
            }))
            .send(),
    )
    .await
    .map_err(|_| QuotaFetchError::retryable("重置卡使用结果未知；为避免重复消费，未自动重试。"))?
    .map_err(|_| QuotaFetchError::retryable("重置卡使用结果未知；为避免重复消费，未自动重试。"))?;
    if !response.status().is_success() {
        let status = response.status();
        let payload = read_optional_error_json(response).await;
        return Err(classify_http_error(status, payload.as_ref(), None));
    }
    let payload = read_bounded_json(response).await?;
    Ok(parse_consume_outcome(&payload))
}

fn parse_reset_credit_details(payload: &Value, account_id: &str) -> ResetCreditDetails {
    let source = payload
        .get("data")
        .filter(|value| value.is_object())
        .unwrap_or(payload);
    let mut summary = parse_reset_credit_summary(source);
    let credits = source
        .get("credits")
        .or_else(|| {
            source
                .get("rate_limit_reset_credits")
                .and_then(|value| value.get("credits"))
        })
        .or_else(|| {
            source
                .get("rateLimitResetCredits")
                .and_then(|value| value.get("credits"))
        })
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(parse_credit).collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(available_count) = summary.available_count {
        summary.details_status = if credits
            .iter()
            .filter(|credit| credit.status.as_deref() == Some("available"))
            .count()
            >= available_count as usize
        {
            ResetCreditDetailsStatus::Complete
        } else {
            ResetCreditDetailsStatus::Partial
        };
    }
    ResetCreditDetails {
        account_id: account_id.into(),
        summary,
        credits,
    }
}

fn parse_credit(value: &Value) -> Option<ResetCredit> {
    let id = value.get("id")?.as_str()?.trim();
    if id.is_empty() || id.len() > 512 {
        return None;
    }
    let text = |name: &str, camel: &str| {
        value
            .get(name)
            .or_else(|| value.get(camel))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 512)
            .map(str::to_owned)
    };
    Some(ResetCredit {
        id: id.to_owned(),
        reset_type: text("reset_type", "resetType"),
        status: text("status", "status"),
        granted_at: value
            .get("granted_at")
            .or_else(|| value.get("grantedAt"))
            .and_then(parse_timestamp),
        expires_at: value
            .get("expires_at")
            .or_else(|| value.get("expiresAt"))
            .and_then(parse_timestamp),
        title: text("title", "title"),
        description: text("description", "description"),
    })
}

fn parse_consume_outcome(payload: &Value) -> ResetCreditConsumeOutcome {
    match payload
        .get("outcome")
        .or_else(|| payload.pointer("/data/outcome"))
        .and_then(Value::as_str)
    {
        Some("reset") => ResetCreditConsumeOutcome::Reset,
        Some("already_redeemed") | Some("alreadyRedeemed") => {
            ResetCreditConsumeOutcome::AlreadyRedeemed
        }
        Some("nothing_to_reset") | Some("nothingToReset") => {
            ResetCreditConsumeOutcome::NothingToReset
        }
        Some("no_credit") | Some("noCredit") => ResetCreditConsumeOutcome::NoCredit,
        Some("failed") => ResetCreditConsumeOutcome::Failed,
        _ => ResetCreditConsumeOutcome::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_snake_case_details_and_marks_truncated_lists_partial() {
        let details = parse_reset_credit_details(
            &json!({
                "available_count": 2,
                "credits": [{"id":"credit-1","status":"available","expires_at":"2026-09-30T00:00:00Z","title":"优先重置"}]
            }),
            "account-1",
        );
        assert_eq!(details.summary.available_count, Some(2));
        assert_eq!(
            details.summary.details_status,
            ResetCreditDetailsStatus::Partial
        );
        assert_eq!(details.credits[0].expires_at, Some(1_790_726_400));
    }

    #[test]
    fn maps_only_documented_consume_outcomes_and_keeps_unknown_safe() {
        assert_eq!(
            parse_consume_outcome(&json!({"outcome":"reset"})),
            ResetCreditConsumeOutcome::Reset
        );
        assert_eq!(
            parse_consume_outcome(&json!({"outcome":"already_redeemed"})),
            ResetCreditConsumeOutcome::AlreadyRedeemed
        );
        assert_eq!(
            parse_consume_outcome(&json!({"outcome":"new_server_value"})),
            ResetCreditConsumeOutcome::Unknown
        );
    }
}
