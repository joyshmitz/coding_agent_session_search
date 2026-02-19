//! Connectors for agent histories.

// Re-export normalized types and connector infrastructure from franken_agent_detection.
pub use franken_agent_detection::{
    Connector,
    DetectionResult,
    // Scan & provenance types
    LOCAL_SOURCE_ID,
    NormalizedConversation,
    NormalizedMessage,
    NormalizedSnippet,
    Origin,
    PathMapping,
    // Connector infrastructure
    PathTrie,
    Platform,
    ScanContext,
    ScanRoot,
    SourceKind,
    WorkspaceCache,
    file_modified_since,
    flatten_content,
    franken_detection_for_connector,
    parse_timestamp,
    reindex_messages,
};

pub mod aider;
pub mod amp;
pub mod chatgpt;
pub mod claude_code;
pub mod clawdbot;
pub mod cline;
pub mod codex;
pub mod copilot;
pub mod cursor;
pub mod factory;
pub mod gemini;
pub mod openclaw;
pub mod opencode;
pub mod pi_agent;
pub mod vibe;

// -------------------------------------------------------------------------
// Token Extraction — Per-message token usage extraction from raw data
// -------------------------------------------------------------------------

/// Quality indicator for extracted token data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TokenDataSource {
    /// Actual token counts from API response usage block.
    Api,
    /// Estimated from content character count (~4 chars per token).
    #[default]
    Estimated,
}

impl TokenDataSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Estimated => "estimated",
        }
    }
}

/// Extracted token usage from a single message's raw data.
#[derive(Debug, Clone, Default)]
pub struct ExtractedTokenUsage {
    pub model_name: Option<String>,
    pub provider: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
    pub thinking_tokens: Option<i64>,
    pub service_tier: Option<String>,
    pub has_tool_calls: bool,
    pub tool_call_count: u32,
    pub data_source: TokenDataSource,
}

impl ExtractedTokenUsage {
    /// Compute total tokens from all components.
    pub fn total_tokens(&self) -> Option<i64> {
        let mut total: i64 = 0;
        let mut has_any = false;
        for v in [
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_creation_tokens,
        ]
        .into_iter()
        .flatten()
        {
            total = total.saturating_add(v);
            has_any = true;
        }
        if has_any { Some(total) } else { None }
    }

    /// Whether this extraction has any meaningful token data.
    pub fn has_token_data(&self) -> bool {
        self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cache_read_tokens.is_some()
            || self.cache_creation_tokens.is_some()
    }
}

/// Normalized model identification.
#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub family: String,
    pub tier: String,
    pub provider: String,
}

/// Normalize raw model strings into (family, tier, provider).
pub fn normalize_model(raw: &str) -> ModelInfo {
    let lower = raw.to_lowercase();

    // Claude models: claude-opus-4-6, claude-sonnet-4-5-20250929, claude-haiku-4-5-20251001
    if lower.starts_with("claude") {
        let tier = if lower.contains("opus") {
            "opus"
        } else if lower.contains("sonnet") {
            "sonnet"
        } else if lower.contains("haiku") {
            "haiku"
        } else {
            "unknown"
        };
        return ModelInfo {
            family: "claude".into(),
            tier: tier.into(),
            provider: "anthropic".into(),
        };
    }

    // OpenAI o-series: o3, o4-mini, o1-preview
    if lower.starts_with("o1") || lower.starts_with("o3") || lower.starts_with("o4") {
        return ModelInfo {
            family: "gpt".into(),
            tier: lower.split('-').next().unwrap_or(&lower).into(),
            provider: "openai".into(),
        };
    }

    // GPT models: gpt-4o, gpt-4-turbo, gpt-4.1
    if lower.starts_with("gpt") {
        let tier = lower.strip_prefix("gpt-").unwrap_or(&lower).to_string();
        return ModelInfo {
            family: "gpt".into(),
            tier,
            provider: "openai".into(),
        };
    }

    // Gemini models: gemini-2.0-flash, gemini-2.0-pro
    if lower.starts_with("gemini") {
        let tier = if lower.contains("flash") {
            "flash"
        } else if lower.contains("pro") {
            "pro"
        } else if lower.contains("ultra") {
            "ultra"
        } else {
            "unknown"
        };
        return ModelInfo {
            family: "gemini".into(),
            tier: tier.into(),
            provider: "google".into(),
        };
    }

    // Fallback
    ModelInfo {
        family: "unknown".into(),
        tier: raw.to_string(),
        provider: "unknown".into(),
    }
}

/// Extract token usage from a Claude Code message's raw data.
///
/// Claude Code stores rich usage data in the message JSON:
/// ```json
/// {"message": {"model": "claude-opus-4-6", "usage": {
///     "input_tokens": 3, "output_tokens": 10,
///     "cache_read_input_tokens": 19152,
///     "cache_creation_input_tokens": 7997,
///     "service_tier": "standard"
/// }}}
/// ```
pub fn extract_claude_code_tokens(extra: &serde_json::Value) -> ExtractedTokenUsage {
    let model_name = extra
        .pointer("/message/model")
        .and_then(|v| v.as_str())
        .map(String::from);

    let provider = model_name
        .as_deref()
        .map(|name| normalize_model(name).provider);

    let u = extra.pointer("/message/usage");
    let input_tokens = u
        .and_then(|u| u.get("input_tokens"))
        .and_then(|v| v.as_i64());
    let output_tokens = u
        .and_then(|u| u.get("output_tokens"))
        .and_then(|v| v.as_i64());
    let cache_read_tokens = u
        .and_then(|u| u.get("cache_read_input_tokens"))
        .and_then(|v| v.as_i64());
    let cache_creation_tokens = u
        .and_then(|u| u.get("cache_creation_input_tokens"))
        .and_then(|v| v.as_i64());
    let service_tier = u
        .and_then(|u| u.get("service_tier"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let has_api_data = input_tokens.is_some()
        || output_tokens.is_some()
        || cache_read_tokens.is_some()
        || cache_creation_tokens.is_some();

    // Count tool_use blocks in message.content array
    let (has_tool_calls, tool_call_count) =
        if let Some(content_arr) = extra.pointer("/message/content").and_then(|v| v.as_array()) {
            let count = content_arr
                .iter()
                .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                .count() as u32;
            (count > 0, count)
        } else {
            (false, 0)
        };

    ExtractedTokenUsage {
        model_name,
        provider,
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_creation_tokens,
        thinking_tokens: None,
        service_tier,
        has_tool_calls,
        tool_call_count,
        data_source: if has_api_data {
            TokenDataSource::Api
        } else {
            TokenDataSource::Estimated
        },
    }
}

/// Extract token usage from a Codex message's raw data.
pub fn extract_codex_tokens(extra: &serde_json::Value) -> ExtractedTokenUsage {
    let mut input_tokens = None;
    let mut output_tokens = None;
    let mut data_source = TokenDataSource::Estimated;

    // Preferred shape: token_count usage attached to assistant message extra.
    if let Some(attached) = extra.pointer("/cass/token_usage") {
        input_tokens = attached.get("input_tokens").and_then(|v| v.as_i64());
        output_tokens = attached
            .get("output_tokens")
            .and_then(|v| v.as_i64())
            .or_else(|| attached.get("tokens").and_then(|v| v.as_i64()));

        let source = attached.get("data_source").and_then(|v| v.as_str());
        if source == Some("api") || input_tokens.is_some() || output_tokens.is_some() {
            data_source = TokenDataSource::Api;
        }
    }

    // Legacy shape: raw event_msg token_count payload.
    if input_tokens.is_none()
        && output_tokens.is_none()
        && extra.get("type").and_then(|v| v.as_str()) == Some("event_msg")
        && let Some(payload) = extra.get("payload")
        && payload.get("type").and_then(|v| v.as_str()) == Some("token_count")
    {
        input_tokens = payload.get("input_tokens").and_then(|v| v.as_i64());
        output_tokens = payload
            .get("output_tokens")
            .and_then(|v| v.as_i64())
            .or_else(|| payload.get("tokens").and_then(|v| v.as_i64()));
        data_source = TokenDataSource::Api;
    }

    // Codex response_item may have model info
    let model_name = extra
        .get("model")
        .or_else(|| extra.pointer("/response/model"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let provider = model_name
        .as_deref()
        .map(|name| normalize_model(name).provider);

    ExtractedTokenUsage {
        model_name,
        provider,
        input_tokens,
        output_tokens,
        data_source,
        ..Default::default()
    }
}

/// Estimate tokens from content length for agents that don't provide token data.
/// Uses the ~4 characters per token heuristic.
pub fn estimate_tokens_from_content(content: &str, role: &str) -> ExtractedTokenUsage {
    let char_count = content.len() as i64;
    let estimated = char_count / 4; // Conservative: ~4 chars per token

    let mut usage = ExtractedTokenUsage {
        data_source: TokenDataSource::Estimated,
        ..Default::default()
    };

    match role {
        "user" => usage.input_tokens = Some(estimated),
        "assistant" | "agent" => usage.output_tokens = Some(estimated),
        _ => usage.output_tokens = Some(estimated),
    }

    usage
}

/// Extract token usage from a message, dispatching by agent type.
///
/// For agents with rich token data (Claude Code, Codex), extracts from the raw
/// source JSON stored in `extra`. For others, falls back to content-length estimation.
pub fn extract_tokens_for_agent(
    agent_slug: &str,
    extra: &serde_json::Value,
    content: &str,
    role: &str,
) -> ExtractedTokenUsage {
    let extracted = match agent_slug {
        "claude_code" => extract_claude_code_tokens(extra),
        "codex" => extract_codex_tokens(extra),
        // Agents that may have model names but no token counts
        "cursor" | "pi_agent" | "factory" | "opencode" | "gemini" => {
            let model_name = extra
                .get("model")
                .or_else(|| extra.pointer("/message/model"))
                .or_else(|| extra.pointer("/modelConfig/modelName"))
                .or_else(|| extra.get("modelType"))
                .or_else(|| extra.get("modelID"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let provider = model_name
                .as_deref()
                .map(|name| normalize_model(name).provider);
            ExtractedTokenUsage {
                model_name,
                provider,
                ..Default::default()
            }
        }
        // All other agents: no token data available, skip extraction
        _ => ExtractedTokenUsage::default(),
    };

    // If no API token data was found, fall back to content-length estimation
    if !extracted.has_token_data() && !content.is_empty() {
        let mut estimated = estimate_tokens_from_content(content, role);
        // Preserve any model info that was found
        estimated.model_name = extracted.model_name;
        estimated.provider = extracted.provider;
        estimated.has_tool_calls = extracted.has_tool_calls;
        estimated.tool_call_count = extracted.tool_call_count;
        estimated.service_tier = extracted.service_tier;
        return estimated;
    }

    extracted
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Token extraction tests
    // =========================================================================

    #[test]
    fn normalize_model_claude_opus() {
        let info = normalize_model("claude-opus-4-6");
        assert_eq!(info.family, "claude");
        assert_eq!(info.tier, "opus");
        assert_eq!(info.provider, "anthropic");
    }

    #[test]
    fn normalize_model_claude_sonnet() {
        let info = normalize_model("claude-sonnet-4-5-20250929");
        assert_eq!(info.family, "claude");
        assert_eq!(info.tier, "sonnet");
        assert_eq!(info.provider, "anthropic");
    }

    #[test]
    fn normalize_model_gpt4o() {
        let info = normalize_model("gpt-4o");
        assert_eq!(info.family, "gpt");
        assert_eq!(info.tier, "4o");
        assert_eq!(info.provider, "openai");
    }

    #[test]
    fn normalize_model_o3() {
        let info = normalize_model("o3");
        assert_eq!(info.family, "gpt");
        assert_eq!(info.tier, "o3");
        assert_eq!(info.provider, "openai");
    }

    #[test]
    fn normalize_model_gemini_flash() {
        let info = normalize_model("gemini-2.0-flash");
        assert_eq!(info.family, "gemini");
        assert_eq!(info.tier, "flash");
        assert_eq!(info.provider, "google");
    }

    #[test]
    fn normalize_model_unknown() {
        let info = normalize_model("llama-3-70b");
        assert_eq!(info.family, "unknown");
        assert_eq!(info.provider, "unknown");
    }

    #[test]
    fn extract_claude_code_tokens_full() {
        let raw: serde_json::Value = serde_json::json!({
            "message": {
                "model": "claude-opus-4-6",
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 500,
                    "cache_read_input_tokens": 20000,
                    "cache_creation_input_tokens": 5000,
                    "service_tier": "standard"
                },
                "content": [
                    {"type": "text", "text": "Hello"},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/foo"}}
                ]
            }
        });

        let usage = extract_claude_code_tokens(&raw);
        assert_eq!(usage.model_name.as_deref(), Some("claude-opus-4-6"));
        assert_eq!(usage.provider.as_deref(), Some("anthropic"));
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(500));
        assert_eq!(usage.cache_read_tokens, Some(20000));
        assert_eq!(usage.cache_creation_tokens, Some(5000));
        assert_eq!(usage.service_tier.as_deref(), Some("standard"));
        assert_eq!(usage.data_source, TokenDataSource::Api);
        assert!(usage.has_tool_calls);
        assert_eq!(usage.tool_call_count, 1);
        assert_eq!(usage.total_tokens(), Some(25600));
    }

    #[test]
    fn extract_claude_code_tokens_no_usage() {
        let raw: serde_json::Value = serde_json::json!({
            "type": "user",
            "content": "Hello"
        });
        let usage = extract_claude_code_tokens(&raw);
        assert!(!usage.has_token_data());
        assert_eq!(usage.data_source, TokenDataSource::Estimated);
    }

    #[test]
    fn extract_codex_tokens_from_attached_token_usage() {
        let raw: serde_json::Value = serde_json::json!({
            "type": "response_item",
            "payload": {
                "role": "assistant",
                "content": "answer"
            },
            "cass": {
                "token_usage": {
                    "input_tokens": 111,
                    "output_tokens": 222,
                    "data_source": "api"
                }
            }
        });

        let usage = extract_codex_tokens(&raw);
        assert_eq!(usage.input_tokens, Some(111));
        assert_eq!(usage.output_tokens, Some(222));
        assert_eq!(usage.data_source, TokenDataSource::Api);
    }

    #[test]
    fn extract_codex_tokens_from_legacy_event_msg_payload() {
        let raw: serde_json::Value = serde_json::json!({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "input_tokens": 10,
                "output_tokens": 20
            }
        });

        let usage = extract_codex_tokens(&raw);
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(20));
        assert_eq!(usage.data_source, TokenDataSource::Api);
    }

    #[test]
    fn extract_codex_tokens_legacy_tokens_fallback() {
        let raw: serde_json::Value = serde_json::json!({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "tokens": 77
            }
        });

        let usage = extract_codex_tokens(&raw);
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, Some(77));
        assert_eq!(usage.data_source, TokenDataSource::Api);
    }

    #[test]
    fn estimate_tokens_user_message() {
        let usage = estimate_tokens_from_content("Hello, this is a test message!", "user");
        assert!(usage.input_tokens.unwrap() > 0);
        assert!(usage.output_tokens.is_none());
        assert_eq!(usage.data_source, TokenDataSource::Estimated);
    }

    #[test]
    fn estimate_tokens_assistant_message() {
        let usage =
            estimate_tokens_from_content("Here is my response to your question.", "assistant");
        assert!(usage.input_tokens.is_none());
        assert!(usage.output_tokens.unwrap() > 0);
        assert_eq!(usage.data_source, TokenDataSource::Estimated);
    }

    #[test]
    fn extract_tokens_for_agent_claude_with_data() {
        let raw: serde_json::Value = serde_json::json!({
            "message": {
                "model": "claude-sonnet-4-5-20250929",
                "usage": {
                    "input_tokens": 50,
                    "output_tokens": 200
                },
                "content": [{"type": "text", "text": "Response"}]
            }
        });
        let usage = extract_tokens_for_agent("claude_code", &raw, "Response", "assistant");
        assert_eq!(usage.data_source, TokenDataSource::Api);
        assert_eq!(usage.input_tokens, Some(50));
        assert_eq!(usage.output_tokens, Some(200));
    }

    #[test]
    fn extract_tokens_for_agent_unknown_falls_back() {
        let raw = serde_json::Value::Null;
        let usage = extract_tokens_for_agent("aider", &raw, "Some content here", "assistant");
        assert_eq!(usage.data_source, TokenDataSource::Estimated);
        assert!(usage.output_tokens.unwrap() > 0);
    }
}
