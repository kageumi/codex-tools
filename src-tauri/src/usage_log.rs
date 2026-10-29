use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(test)]
const MAX_RECORD_LINE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsageQuality {
    Complete,
    Partial,
    CompatibleFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedUsageEvent {
    pub ordinal: u64,
    pub occurred_at_ms: i64,
    pub model: String,
    pub model_provider: Option<String>,
    pub usage: TokenUsage,
    pub quality: UsageQuality,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum UsageBoundaryState {
    #[default]
    Regular,
    AwaitingSubagentTaskStart,
    AwaitingSubagentBoundaryMarker,
    Started,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ParserState {
    pub rollout_id: Option<String>,
    pub model_provider: Option<String>,
    pub model: Option<String>,
    pub next_event_ordinal: u64,
    pub usage_boundary: UsageBoundaryState,
    pub boundary_marker_required: bool,
    pub last_cumulative_usage: Option<TokenUsage>,
    pub parent_rollout_id: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct ParsedRollout {
    pub rollout_id: Option<String>,
    pub events: Vec<ParsedUsageEvent>,
    pub warnings: Vec<String>,
    pub state: ParserState,
}

pub(crate) enum LineResult {
    Ignored,
    Event(ParsedUsageEvent),
    SkippedInheritedUsage,
    Warning(String),
}

#[cfg(test)]
pub(crate) fn parse_rollout_text(text: &str) -> ParsedRollout {
    let boundary_marker_required = text
        .lines()
        .any(|line| is_inter_agent_trigger_line(line.as_bytes()));
    let mut state = ParserState {
        boundary_marker_required,
        ..ParserState::default()
    };
    let mut output = ParsedRollout::default();

    for segment in text.split_inclusive('\n') {
        let complete_line = segment.ends_with('\n');
        let line = segment.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > MAX_RECORD_LINE_BYTES {
            if complete_line {
                output.warnings.push("跳过超过 8 MB 的会话日志行。".into());
            }
            continue;
        }

        match parse_line(line.as_bytes(), &mut state) {
            Ok(LineResult::Ignored) => {}
            Ok(LineResult::Event(event)) => output.events.push(event),
            Ok(LineResult::SkippedInheritedUsage) => {}
            Ok(LineResult::Warning(warning)) => output.warnings.push(warning),
            Err(error) if !complete_line => {
                // A final line may still be being written by Codex. It is retried
                // from the cursor on the next refresh and must not be reported as
                // a permanent warning.
                let _ = error;
            }
            Err(error) => output.warnings.push(error),
        }
    }

    output.rollout_id = state.rollout_id.clone();
    output.state = state;
    output
}

pub(crate) fn parse_line(line: &[u8], state: &mut ParserState) -> Result<LineResult, String> {
    let value: Value =
        serde_json::from_slice(line).map_err(|error| format!("会话日志 JSON 无法解析：{error}"))?;
    let record_type = value.get("type").and_then(Value::as_str);
    let payload_type = value
        .get("payload")
        .and_then(Value::as_object)
        .and_then(|payload| payload.get("type"))
        .and_then(Value::as_str);

    match record_type {
        Some("session_meta") => {
            // Child logs contain copied parent metadata after their own header.
            // The file owner is immutable; inherited metadata cannot replace it.
            if state.rollout_id.is_some() {
                return Ok(LineResult::Ignored);
            }
            let payload = value.get("payload").and_then(Value::as_object);
            if let Some(id) = payload
                .and_then(|payload| payload.get("id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                state.rollout_id = Some(id.to_owned());
            }
            if payload
                .and_then(|payload| payload.get("source"))
                .and_then(|source| source.get("subagent"))
                .and_then(|subagent| subagent.get("thread_spawn"))
                .is_some()
            {
                state.usage_boundary = UsageBoundaryState::AwaitingSubagentTaskStart;
                state.parent_rollout_id = payload
                    .and_then(|payload| payload.get("source"))
                    .and_then(|source| source.get("subagent"))
                    .and_then(|subagent| subagent.get("thread_spawn"))
                    .and_then(|spawn| spawn.get("parent_thread_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            if let Some(provider) = payload
                .and_then(|payload| {
                    payload
                        .get("model_provider")
                        .or_else(|| payload.get("model_provider_id"))
                })
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
            {
                state.model_provider = Some(provider.to_owned());
            }
            Ok(LineResult::Ignored)
        }
        Some("event_msg") if payload_type == Some("task_started") => {
            if state.usage_boundary == UsageBoundaryState::AwaitingSubagentTaskStart {
                // task_started identifies the child, but the no-marker format
                // still needs the following valid turn_context to establish
                // the boundary.
                state.usage_boundary = UsageBoundaryState::AwaitingSubagentBoundaryMarker;
            }
            Ok(LineResult::Ignored)
        }
        Some("inter_agent_communication_metadata")
            if value
                .get("payload")
                .and_then(|payload| payload.get("trigger_turn"))
                .and_then(Value::as_bool)
                == Some(true) =>
        {
            if state.usage_boundary == UsageBoundaryState::AwaitingSubagentBoundaryMarker {
                state.usage_boundary = UsageBoundaryState::Started;
            }
            Ok(LineResult::Ignored)
        }
        Some("turn_context") => {
            let payload = value.get("payload").and_then(Value::as_object);
            if let Some(model) = payload
                .and_then(|payload| payload.get("model"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|model| !model.is_empty())
            {
                state.model = Some(model.to_owned());
            }
            if let Some(provider) = payload
                .and_then(|payload| {
                    payload
                        .get("model_provider")
                        .or_else(|| payload.get("model_provider_id"))
                })
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
            {
                state.model_provider = Some(provider.to_owned());
            }
            if !state.boundary_marker_required
                && state.usage_boundary == UsageBoundaryState::AwaitingSubagentBoundaryMarker
            {
                state.usage_boundary = UsageBoundaryState::Started;
            }
            Ok(LineResult::Ignored)
        }
        Some("event_msg") if payload_type == Some("thread_settings_applied") => {
            let settings = value
                .get("payload")
                .and_then(|payload| payload.get("thread_settings"));
            if let Some(model) = settings
                .and_then(|settings| settings.get("model"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|model| !model.is_empty())
            {
                state.model = Some(model.to_owned());
            }
            if let Some(provider) = settings
                .and_then(|settings| {
                    settings
                        .get("model_provider_id")
                        .or_else(|| settings.get("model_provider"))
                })
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
            {
                state.model_provider = Some(provider.to_owned());
            }
            Ok(LineResult::Ignored)
        }
        Some("token_count") | Some("event_msg")
            if payload_type == Some("token_count") || record_type == Some("token_count") =>
        {
            let ordinal = state.next_event_ordinal;
            state.next_event_ordinal = state.next_event_ordinal.saturating_add(1);

            if matches!(
                state.usage_boundary,
                UsageBoundaryState::AwaitingSubagentTaskStart
                    | UsageBoundaryState::AwaitingSubagentBoundaryMarker
            ) {
                return Ok(LineResult::SkippedInheritedUsage);
            }

            let Some(info) = value
                .get("payload")
                .and_then(Value::as_object)
                .and_then(|payload| payload.get("info"))
            else {
                return Ok(LineResult::Ignored);
            };
            if info.is_null() {
                return Ok(LineResult::Ignored);
            }
            let usage = info
                .as_object()
                .and_then(|info| info.get("last_token_usage"))
                .filter(|usage| !usage.is_null())
                .ok_or_else(|| "Token 事件缺少 last_token_usage。".to_owned())?;

            let timestamp = value
                .get("timestamp")
                .or_else(|| {
                    value
                        .get("payload")
                        .and_then(|payload| payload.get("timestamp"))
                })
                .and_then(parse_timestamp)
                .ok_or_else(|| "Token 事件缺少有效时间戳。".to_owned());
            let timestamp = match timestamp {
                Ok(timestamp) => timestamp,
                Err(error) => return Ok(LineResult::Warning(error)),
            };

            let (usage, quality) = parse_usage(usage)?;
            let (usage, quality) = match incremental_usage(info, usage, quality, state) {
                Ok(Some(usage)) => usage,
                Ok(None) => return Ok(LineResult::Ignored),
                Err(warning) => return Ok(LineResult::Warning(warning)),
            };
            Ok(LineResult::Event(ParsedUsageEvent {
                ordinal,
                occurred_at_ms: timestamp,
                model: state.model.clone().unwrap_or_else(|| "unknown".into()),
                model_provider: state.model_provider.clone(),
                usage,
                quality,
            }))
        }
        _ => Ok(LineResult::Ignored),
    }
}

fn incremental_usage(
    info: &Value,
    last: TokenUsage,
    quality: UsageQuality,
    state: &mut ParserState,
) -> Result<Option<(TokenUsage, UsageQuality)>, String> {
    let Some(raw) = info
        .get("total_token_usage")
        .filter(|value| !value.is_null())
    else {
        // Do not bridge a gap whose cumulative baseline is unknown.
        state.last_cumulative_usage = None;
        return Ok(Some((last, quality)));
    };
    let (current, _) = parse_usage(raw)?;
    if [
        ["input_tokens", "inputTokens"],
        ["output_tokens", "outputTokens"],
        ["total_tokens", "totalTokens"],
    ]
    .iter()
    .any(|names| read_count(raw, names).1)
        || current.input_tokens.checked_add(current.output_tokens) != Some(current.total_tokens)
    {
        state.last_cumulative_usage = None;
        return Err("累计 Token 快照字段缺失或不一致，未计入该快照。".into());
    }
    let previous = state.last_cumulative_usage.replace(current);
    let Some(previous) = previous else {
        // The first cumulative value may include usage before collection began.
        return Ok(Some((last, quality)));
    };
    if current == previous {
        return Ok(None);
    }
    let delta = (|| {
        Some(TokenUsage {
            input_tokens: current.input_tokens.checked_sub(previous.input_tokens)?,
            cached_input_tokens: current
                .cached_input_tokens
                .checked_sub(previous.cached_input_tokens)?,
            cache_write_input_tokens: current
                .cache_write_input_tokens
                .checked_sub(previous.cache_write_input_tokens)?,
            output_tokens: current.output_tokens.checked_sub(previous.output_tokens)?,
            reasoning_output_tokens: current
                .reasoning_output_tokens
                .checked_sub(previous.reasoning_output_tokens)?,
            total_tokens: current.total_tokens.checked_sub(previous.total_tokens)?,
        })
    })();
    match delta {
        Some(delta) if delta.total_tokens == 0 => Ok(None),
        Some(delta) => Ok(Some((delta, quality))),
        // A reset is unambiguous only when the new counter equals this request.
        None if current == last => Ok(Some((last, UsageQuality::CompatibleFallback))),
        None => {
            Err("累计 Token 计数回退且无法确认重置边界，未计入该快照；后续从新基线继续。".into())
        }
    }
}

fn parse_usage(value: &Value) -> Result<(TokenUsage, UsageQuality), String> {
    let mut quality = UsageQuality::Complete;
    let (input_tokens, input_missing) = read_count(value, &["input_tokens", "inputTokens"]);
    let (cached_input_tokens, cached_missing) =
        read_count(value, &["cached_input_tokens", "cachedInputTokens"]);
    let (cache_write_input_tokens, cache_write_missing) = read_count(
        value,
        &["cache_write_input_tokens", "cacheWriteInputTokens"],
    );
    let (output_tokens, output_missing) = read_count(value, &["output_tokens", "outputTokens"]);
    let (reasoning_output_tokens, reasoning_missing) =
        read_count(value, &["reasoning_output_tokens", "reasoningOutputTokens"]);
    let (total_tokens, total_missing) = read_count(value, &["total_tokens", "totalTokens"]);

    if input_missing || cached_missing || cache_write_missing || output_missing || reasoning_missing
    {
        quality = UsageQuality::Partial;
    }
    let total_tokens = if total_missing {
        quality = UsageQuality::CompatibleFallback;
        input_tokens.saturating_add(output_tokens)
    } else {
        total_tokens
    };

    Ok((
        TokenUsage {
            input_tokens,
            cached_input_tokens,
            cache_write_input_tokens,
            output_tokens,
            reasoning_output_tokens,
            total_tokens,
        },
        quality,
    ))
}

fn read_count(value: &Value, names: &[&str]) -> (u64, bool) {
    let Some(raw) = names.iter().find_map(|name| value.get(*name)) else {
        return (0, true);
    };
    match raw.as_u64() {
        Some(value) => (value, false),
        None => (0, true),
    }
}

fn parse_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::String(value) => DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.timestamp_millis()),
        Value::Number(value) => {
            let raw = value.as_i64()? as i128;
            let millis = if raw.unsigned_abs() < 100_000_000_000 {
                raw.checked_mul(1_000)?
            } else {
                raw
            };
            i64::try_from(millis).ok()
        }
        _ => None,
    }
}

pub(crate) fn is_inter_agent_trigger_line(line: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(line) else {
        return false;
    };
    value.get("type").and_then(Value::as_str) == Some("inter_agent_communication_metadata")
        && value
            .get("payload")
            .and_then(|payload| payload.get("trigger_turn"))
            .and_then(Value::as_bool)
            == Some(true)
}

#[cfg(test)]
mod tests {
    use super::{TokenUsage, parse_rollout_text};

    #[test]
    fn repeated_snapshots_and_confirmed_resets_do_not_inflate_usage() {
        let usage = serde_json::json!({"input_tokens":10,"output_tokens":2,"total_tokens":12});
        let lines = [1, 1, 2, 1, 2].map(|multiple| {
            serde_json::json!({"type":"token_count","timestamp":"2026-08-01T10:00:01Z",
                "payload":{"info":{"last_token_usage":usage,
                    "total_token_usage":{"input_tokens":10*multiple,"output_tokens":2*multiple,"total_tokens":12*multiple}}}
            }).to_string()
        }).join("\n") + "\n";
        let parsed = parse_rollout_text(&lines);
        assert_eq!(parsed.events.len(), 4);
        assert_eq!(
            parsed
                .events
                .iter()
                .map(|event| event.usage.total_tokens)
                .sum::<u64>(),
            48
        );
        assert!(parsed.warnings.is_empty());
    }

    #[test]
    fn ambiguous_counter_rollback_is_reported_and_rebaselined() {
        let lines = [(100, 10), (50, 10), (60, 10)].map(|(total, last)| {
            serde_json::json!({"type":"token_count","timestamp":"2026-08-01T10:00:01Z",
                "payload":{"info":{"last_token_usage":{"input_tokens":last,"output_tokens":0,"total_tokens":last},
                    "total_token_usage":{"input_tokens":total,"output_tokens":0,"total_tokens":total}}}
            }).to_string()
        }).join("\n") + "\n";
        let parsed = parse_rollout_text(&lines);
        assert_eq!(parsed.events.len(), 2);
        assert_eq!(parsed.warnings.len(), 1);
        assert_eq!(parsed.events[1].usage.total_tokens, 10);
    }

    #[test]
    fn parses_model_context_and_incremental_token_usage() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-1","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"turn_context","timestamp":"2026-08-01T10:00:00Z","payload":{"model":"gpt-5.6-sol"}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:01Z","payload":{"info":{"last_token_usage":{"input_tokens":100,"cached_input_tokens":20,"cache_write_input_tokens":3,"output_tokens":8,"reasoning_output_tokens":2,"total_tokens":108},"total_token_usage":{"input_tokens":1000,"output_tokens":80,"total_tokens":1080}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.rollout_id.as_deref(), Some("rollout-1"));
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].model, "gpt-5.6-sol");
        assert_eq!(parsed.events[0].model_provider.as_deref(), Some("openai"));
        assert_eq!(parsed.events[0].ordinal, 0);
        assert_eq!(
            parsed.events[0].usage,
            TokenUsage {
                input_tokens: 100,
                cached_input_tokens: 20,
                cache_write_input_tokens: 3,
                output_tokens: 8,
                reasoning_output_tokens: 2,
                total_tokens: 108,
            }
        );
    }

    #[test]
    fn parses_codex_event_msg_token_count_shape() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-event-msg","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-luna"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-01T10:00:01.000Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":11,"cached_input_tokens":2,"cache_write_input_tokens":1,"output_tokens":4,"reasoning_output_tokens":1,"total_tokens":15}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].model, "gpt-5.6-luna");
        assert_eq!(parsed.events[0].usage.total_tokens, 15);
    }

    #[test]
    fn ignores_token_count_without_info_snapshot() {
        let text = concat!(
            r#"{"timestamp":"2026-08-23T10:52:53Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex"}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert!(parsed.events.is_empty());
        assert!(parsed.warnings.is_empty());
        assert_eq!(parsed.state.next_event_ordinal, 2);
    }

    #[test]
    fn warns_for_malformed_info_object_without_last_token_usage() {
        let text = concat!(
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:01Z","payload":{"info":{}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{"info":{"last_token_usage":null}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert!(parsed.events.is_empty());
        assert_eq!(parsed.warnings.len(), 2);
        assert!(
            parsed
                .warnings
                .iter()
                .all(|warning| warning == "Token 事件缺少 last_token_usage。")
        );
    }

    #[test]
    fn continues_after_empty_token_count_snapshot() {
        let text = concat!(
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:01Z","payload":{"info":{"last_token_usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{"info":null}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:03Z","payload":{"info":{"last_token_usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 2);
        assert!(parsed.warnings.is_empty());
        assert_eq!(parsed.events[0].ordinal, 0);
        assert_eq!(parsed.events[1].ordinal, 2);
        assert_eq!(parsed.events[1].usage.total_tokens, 5);
    }

    #[test]
    fn applies_provider_changes_from_thread_settings_events() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-provider-switch","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-luna","model_provider_id":"custom"}}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-luna"}}"#,
            "\n",
            r#"{"type":"event_msg","timestamp":"2026-08-01T10:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":11,"output_tokens":4,"total_tokens":15}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].model, "gpt-5.6-luna");
        assert_eq!(parsed.events[0].model_provider.as_deref(), Some("custom"));
    }

    #[test]
    fn skips_inherited_subagent_usage_until_task_started() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"subagent-rollout","model_provider":"openai","source":{"subagent":{"thread_spawn":{"parent_thread_id":"parent-rollout"}}}}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-sol"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100,"output_tokens":10,"total_tokens":110}}}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-luna","model_provider_id":"custom"}}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-luna"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-02T04:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":11,"output_tokens":4,"total_tokens":15}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].ordinal, 1);
        assert_eq!(parsed.events[0].model, "gpt-5.6-luna");
        assert_eq!(parsed.events[0].model_provider.as_deref(), Some("custom"));
        assert_eq!(
            parsed.state.usage_boundary,
            super::UsageBoundaryState::Started
        );
    }

    #[test]
    fn counts_no_marker_subagent_after_task_started_and_turn_context() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"subagent-no-marker","source":{"subagent":{"thread_spawn":{"parent_thread_id":"parent-rollout"}}}}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-sol"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100,"output_tokens":10,"total_tokens":110}}}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-luna"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-02T04:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":11,"output_tokens":4,"total_tokens":15}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].model, "gpt-5.6-luna");
        assert_eq!(parsed.events[0].ordinal, 1);
        assert_eq!(parsed.events[0].usage.total_tokens, 15);
    }

    #[test]
    fn skips_all_inherited_task_starts_until_inter_agent_boundary() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"subagent-real-boundary","source":{"subagent":{"thread_spawn":{"parent_thread_id":"parent-rollout"}}}}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100,"output_tokens":10,"total_tokens":110}}}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":200,"output_tokens":20,"total_tokens":220}}}}"#,
            "\n",
            r#"{"type":"response_item","payload":{"role":"developer"}}"#,
            "\n",
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            "\n",
            r#"{"type":"world_state","payload":{}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-luna","model_provider":"custom"}}"#,
            "\n",
            r#"{"type":"inter_agent_communication_metadata","payload":{"trigger_turn":true}}"#,
            "\n",
            r#"{"type":"event_msg","timestamp":"2026-08-02T04:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":11,"output_tokens":4,"total_tokens":15}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].usage.total_tokens, 15);
        assert_eq!(parsed.events[0].ordinal, 2);
        assert_eq!(
            parsed.state.usage_boundary,
            super::UsageBoundaryState::Started
        );
    }

    #[test]
    fn uses_last_usage_instead_of_cumulative_total_usage() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-2","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"turn_context","payload":{"model":"gpt-5.6-sol"}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:01Z","payload":{"info":{"last_token_usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7},"total_token_usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{"info":{"last_token_usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13},"total_token_usage":{"input_tokens":14,"output_tokens":6,"total_tokens":20}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 2);
        assert_eq!(parsed.events[0].usage.total_tokens, 7);
        assert_eq!(parsed.events[1].usage.total_tokens, 13);
        assert_eq!(parsed.events[1].ordinal, 1);
    }

    #[test]
    fn keeps_valid_events_and_ignores_an_incomplete_trailing_line() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-3","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:01Z","payload":{"info":{"last_token_usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{"info":{"last_token_usage":{"input_tokens":2"#,
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert!(parsed.warnings.is_empty());
    }

    #[test]
    fn reports_missing_timestamp_and_keeps_event_ordinal_stable() {
        let text = concat!(
            r#"{"type":"session_meta","payload":{"id":"rollout-4","model_provider":"openai"}}"#,
            "\n",
            r#"{"type":"token_count","payload":{"info":{"last_token_usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}}"#,
            "\n",
            r#"{"type":"token_count","timestamp":"2026-08-01T10:00:02Z","payload":{"info":{"last_token_usage":{"input_tokens":4,"output_tokens":1,"total_tokens":5}}}}"#,
            "\n",
        );

        let parsed = parse_rollout_text(text);

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].ordinal, 1);
        assert_eq!(parsed.warnings.len(), 1);
    }
}
