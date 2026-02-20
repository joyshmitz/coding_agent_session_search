//! Connectors for agent histories.

// Re-export normalized types and connector infrastructure from franken_agent_detection.
pub use franken_agent_detection::{
    Connector,
    DetectionResult,
    ExtractedTokenUsage,
    LOCAL_SOURCE_ID,
    ModelInfo,
    // Scan & provenance types
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
    TokenDataSource,
    WorkspaceCache,
    estimate_tokens_from_content,
    extract_claude_code_tokens,
    extract_codex_tokens,
    extract_tokens_for_agent,
    file_modified_since,
    flatten_content,
    franken_detection_for_connector,
    normalize_model,
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
