//! Connectors for agent histories.

use crate::sources::config::Platform;
use crate::sources::provenance::Origin;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub mod aider;
pub mod amp;
pub mod chatgpt;
pub mod claude_code;
pub mod cline;
pub mod codex;
pub mod cursor;
pub mod gemini;
pub mod opencode;
pub mod pi_agent;

/// High-level detection status for a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionResult {
    pub detected: bool,
    pub evidence: Vec<String>,
}

impl DetectionResult {
    pub fn not_found() -> Self {
        Self {
            detected: false,
            evidence: Vec::new(),
        }
    }
}

/// A root directory to scan with associated provenance.
///
/// Part of P2.1 - multi-root support for remote sources.
#[derive(Debug, Clone)]
pub struct ScanRoot {
    /// Path to scan (e.g., ~/.claude, or /data/remotes/work-laptop/mirror/home/.claude)
    pub path: PathBuf,

    /// Provenance for conversations found under this root.
    /// Injected into every conversation scanned from this root.
    pub origin: Origin,

    /// Optional platform hint (affects path interpretation for workspace mapping).
    pub platform: Option<Platform>,

    /// Optional path rewrite rules (src_prefix -> dst_prefix).
    /// Used to map remote workspace paths to local equivalents for display.
    pub workspace_rewrites: Vec<(String, String)>,
}

impl ScanRoot {
    /// Create a local scan root with default provenance.
    pub fn local(path: PathBuf) -> Self {
        Self {
            path,
            origin: Origin::local(),
            platform: None,
            workspace_rewrites: Vec::new(),
        }
    }

    /// Create a remote scan root.
    pub fn remote(path: PathBuf, origin: Origin, platform: Option<Platform>) -> Self {
        Self {
            path,
            origin,
            platform,
            workspace_rewrites: Vec::new(),
        }
    }

    /// Add a workspace rewrite rule.
    pub fn with_rewrite(mut self, src_prefix: impl Into<String>, dst_prefix: impl Into<String>) -> Self {
        self.workspace_rewrites.push((src_prefix.into(), dst_prefix.into()));
        self
    }
}

/// Shared scan context parameters.
#[derive(Debug, Clone)]
pub struct ScanContext {
    /// Primary data directory (cass internal state - where DB and index live).
    pub data_dir: PathBuf,

    /// Scan roots to search for agent logs.
    /// If empty, connectors use their default detection logic (backward compat).
    pub scan_roots: Vec<ScanRoot>,

    /// High-water mark for incremental indexing (milliseconds since epoch).
    pub since_ts: Option<i64>,
}

impl ScanContext {
    /// Create a context for local-only scanning (backward compatible).
    ///
    /// Connectors should use their default detection logic when scan_roots is empty.
    pub fn local_default(data_dir: PathBuf, since_ts: Option<i64>) -> Self {
        Self {
            data_dir,
            scan_roots: Vec::new(),
            since_ts,
        }
    }

    /// Create a context with explicit scan roots.
    pub fn with_roots(data_dir: PathBuf, scan_roots: Vec<ScanRoot>, since_ts: Option<i64>) -> Self {
        Self {
            data_dir,
            scan_roots,
            since_ts,
        }
    }

    /// Legacy accessor for backward compatibility.
    /// Returns data_dir as the "data_root" connectors were using before.
    #[deprecated(note = "Use data_dir directly or check scan_roots for explicit roots")]
    pub fn data_root(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Check if we should use default detection logic (no explicit roots).
    pub fn use_default_detection(&self) -> bool {
        self.scan_roots.is_empty()
    }
}

/// Normalized conversation emitted by connectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedConversation {
    pub agent_slug: String,
    pub external_id: Option<String>,
    pub title: Option<String>,
    pub workspace: Option<PathBuf>,
    pub source_path: PathBuf,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub metadata: serde_json::Value,
    pub messages: Vec<NormalizedMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedMessage {
    pub idx: i64,
    pub role: String,
    pub author: Option<String>,
    pub created_at: Option<i64>,
    pub content: String,
    pub extra: serde_json::Value,
    pub snippets: Vec<NormalizedSnippet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedSnippet {
    pub file_path: Option<PathBuf>,
    pub start_line: Option<i64>,
    pub end_line: Option<i64>,
    pub language: Option<String>,
    pub snippet_text: Option<String>,
}

pub trait Connector {
    fn detect(&self) -> DetectionResult;
    fn scan(&self, ctx: &ScanContext) -> anyhow::Result<Vec<NormalizedConversation>>;
}

/// Check if a file was modified since the given timestamp.
/// Returns true if file should be processed (modified since timestamp or no timestamp given).
/// Uses file modification time (mtime) for comparison.
pub fn file_modified_since(path: &std::path::Path, since_ts: Option<i64>) -> bool {
    match since_ts {
        None => true, // No timestamp filter, process all files
        Some(ts) => {
            // Provide a small slack window to account for filesystem mtime granularity.
            // Some filesystems store mtime with 1s resolution, which can cause updates
            // that happen shortly after a scan to be missed if we compare exact millis.
            let threshold = ts.saturating_sub(1_000);
            // Get file modification time
            std::fs::metadata(path)
                .and_then(|m| m.modified())
                .map(|mt| {
                    mt.duration_since(std::time::UNIX_EPOCH)
                        .map(|d| (d.as_millis() as i64) >= threshold)
                        .unwrap_or(true) // On time error, process the file
                })
                .unwrap_or(true) // On metadata error, process the file
        }
    }
}

/// Parse a timestamp from either i64 milliseconds or ISO-8601 string.
/// Returns milliseconds since Unix epoch, or None if unparseable.
///
/// Handles both legacy integer timestamps and modern ISO-8601 strings like:
/// - `1700000000000` (i64 milliseconds)
/// - `"2025-11-12T18:31:32.217Z"` (ISO-8601 string)
pub fn parse_timestamp(val: &serde_json::Value) -> Option<i64> {
    // Try direct i64 first (legacy format)
    if let Some(ts) = val.as_i64() {
        return Some(ts);
    }
    // Try ISO-8601 string (modern format)
    if let Some(s) = val.as_str() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
        // Fallback: try parsing with explicit UTC format
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
            return Some(dt.and_utc().timestamp_millis());
        }
        // Fallback: try without fractional seconds
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ") {
            return Some(dt.and_utc().timestamp_millis());
        }
    }
    None
}

/// Flatten content that may be a string or array of content blocks.
/// Extracts text from text blocks and tool names from `tool_use` blocks.
///
/// Handles:
/// - Direct string content (e.g., user messages)
/// - Array of content blocks with `{"type": "text", "text": "..."}`
/// - Tool use blocks: `{"type": "tool_use", "name": "Read", "input": {...}}`
/// - Codex `input_text` blocks: `{"type": "input_text", "text": "..."}`
pub fn flatten_content(val: &serde_json::Value) -> String {
    // Direct string content (user messages in Claude Code)
    if let Some(s) = val.as_str() {
        return s.to_string();
    }

    // Array of content blocks (assistant messages)
    if let Some(arr) = val.as_array() {
        let parts: Vec<String> = arr
            .iter()
            .filter_map(|item| {
                let item_type = item.get("type").and_then(|v| v.as_str());

                // Standard text block: {"type": "text", "text": "..."}
                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                    // Only include if it's a text type or has no type (plain text)
                    if item_type.is_none()
                        || item_type == Some("text")
                        || item_type == Some("input_text")
                    {
                        return Some(text.to_string());
                    }
                }

                // Tool use block - include tool name for searchability
                if item_type == Some("tool_use") {
                    let name = item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let desc = item
                        .get("input")
                        .and_then(|i| i.get("description"))
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            item.get("input")
                                .and_then(|i| i.get("file_path"))
                                .and_then(|v| v.as_str())
                        })
                        .unwrap_or("");
                    if desc.is_empty() {
                        return Some(format!("[Tool: {name}]"));
                    }
                    return Some(format!("[Tool: {name} - {desc}]"));
                }

                None
            })
            .collect();
        return parts.join("\n");
    }

    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_root_local_creates_with_defaults() {
        let root = ScanRoot::local(PathBuf::from("/test/path"));
        assert_eq!(root.path, PathBuf::from("/test/path"));
        assert_eq!(root.origin.source_id, "local");
        assert!(root.platform.is_none());
        assert!(root.workspace_rewrites.is_empty());
    }

    #[test]
    fn scan_root_remote_sets_origin() {
        let origin = Origin {
            source_id: "work-laptop".to_string(),
            kind: crate::sources::provenance::SourceKind::Ssh,
            host: Some("work.local".to_string()),
        };
        let root = ScanRoot::remote(
            PathBuf::from("/data/remotes/work"),
            origin.clone(),
            Some(Platform::Linux),
        );
        assert_eq!(root.origin.source_id, "work-laptop");
        assert_eq!(root.platform, Some(Platform::Linux));
    }

    #[test]
    fn scan_root_with_rewrite_adds_rule() {
        let root = ScanRoot::local(PathBuf::from("/test"))
            .with_rewrite("/home/user", "/Users/local");
        assert_eq!(root.workspace_rewrites.len(), 1);
        assert_eq!(root.workspace_rewrites[0], ("/home/user".to_string(), "/Users/local".to_string()));
    }

    #[test]
    fn scan_context_local_default_has_empty_roots() {
        let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
        assert_eq!(ctx.data_dir, PathBuf::from("/data"));
        assert!(ctx.scan_roots.is_empty());
        assert!(ctx.use_default_detection());
    }

    #[test]
    fn scan_context_with_roots_sets_roots() {
        let roots = vec![ScanRoot::local(PathBuf::from("/test"))];
        let ctx = ScanContext::with_roots(PathBuf::from("/data"), roots, Some(1000));
        assert_eq!(ctx.scan_roots.len(), 1);
        assert!(!ctx.use_default_detection());
        assert_eq!(ctx.since_ts, Some(1000));
    }
}
