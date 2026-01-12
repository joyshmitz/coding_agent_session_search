//! Connectors for agent histories.

use crate::sources::config::{PathMapping, Platform};
use crate::sources::provenance::Origin;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// -------------------------------------------------------------------------
// PathTrie: Optimized prefix trie for workspace path rewriting
// -------------------------------------------------------------------------

/// A mapping entry stored at trie nodes.
#[derive(Debug, Clone)]
struct TrieMapping {
    /// Target path prefix to rewrite to.
    to: Box<str>,
    /// Optional agent filter (None = applies to all).
    agents: Option<Vec<String>>,
}

impl TrieMapping {
    fn applies_to_agent(&self, agent: Option<&str>) -> bool {
        match (&self.agents, agent) {
            (None, _) => true,
            (Some(_), None) => true,
            (Some(agents), Some(a)) => agents.iter().any(|allowed| allowed == a),
        }
    }
}

/// Trie node for path component matching.
#[derive(Debug, Default)]
struct PathTrieNode {
    /// Children indexed by path component.
    children: HashMap<Box<str>, PathTrieNode>,
    /// Mappings at this node (multiple mappings can share a prefix with different agent filters).
    mappings: Vec<TrieMapping>,
}

/// Prefix trie optimized for workspace path rewriting.
///
/// Provides O(k) lookup where k is the path depth, instead of O(n) where n is
/// the number of mappings. This is a significant improvement for users with
/// many workspace mappings.
#[derive(Debug, Default)]
pub struct PathTrie {
    root: PathTrieNode,
    /// Lookup count for observability.
    lookup_count: AtomicU64,
    /// Hit count (successful rewrites) for observability.
    hit_count: AtomicU64,
}

impl PathTrie {
    /// Create a new empty trie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a trie from a list of path mappings.
    pub fn from_mappings(mappings: &[PathMapping]) -> Self {
        let mut trie = Self::new();
        for mapping in mappings {
            trie.insert(&mapping.from, &mapping.to, mapping.agents.clone());
        }
        trie
    }

    /// Split a path into components, handling both Unix and Windows separators.
    fn split_path(path: &str) -> Vec<&str> {
        path.split(['/', '\\']).filter(|s| !s.is_empty()).collect()
    }

    /// Insert a path mapping into the trie.
    ///
    /// # Arguments
    /// * `from` - Source path prefix to match.
    /// * `to` - Target path prefix to rewrite to.
    /// * `agents` - Optional agent filter.
    pub fn insert(&mut self, from: &str, to: &str, agents: Option<Vec<String>>) {
        let components = Self::split_path(from);
        let mut current = &mut self.root;

        for component in components {
            current = current.children.entry(component.into()).or_default();
        }

        current.mappings.push(TrieMapping {
            to: to.into(),
            agents,
        });
    }

    /// Lookup and rewrite a path using longest-prefix matching.
    ///
    /// # Arguments
    /// * `path` - The path to potentially rewrite.
    /// * `agent` - Optional agent name for filtering.
    ///
    /// # Returns
    /// The rewritten path if a matching mapping is found, otherwise the original path.
    pub fn lookup(&self, path: &str, agent: Option<&str>) -> String {
        self.lookup_count.fetch_add(1, Ordering::Relaxed);

        let components = Self::split_path(path);
        let mut current = &self.root;
        let mut best_match: Option<(&TrieMapping, usize)> = None;

        // Check root-level mappings (empty prefix)
        for mapping in &current.mappings {
            if mapping.applies_to_agent(agent) {
                best_match = Some((mapping, 0));
            }
        }

        // Walk the trie as deep as possible, tracking the deepest matching mapping
        for (depth, component) in components.iter().enumerate() {
            match current.children.get(*component) {
                Some(child) => {
                    current = child;
                    let current_depth = depth + 1;

                    // Check if this node has a matching mapping
                    for mapping in &current.mappings {
                        if mapping.applies_to_agent(agent) {
                            best_match = Some((mapping, current_depth));
                        }
                    }
                }
                None => break, // No more matches possible
            }
        }

        // Apply the best match if found
        if let Some((mapping, depth)) = best_match {
            self.hit_count.fetch_add(1, Ordering::Relaxed);

            // Reconstruct the remaining path after the matched prefix
            let remaining: Vec<&str> = components.into_iter().skip(depth).collect();
            if remaining.is_empty() {
                return mapping.to.to_string();
            }

            // Use the original separator style from the path
            let sep = if path.contains('\\') { '\\' } else { '/' };
            let remainder = remaining.join(&sep.to_string());

            // Handle trailing separator in the target
            if mapping.to.ends_with('/') || mapping.to.ends_with('\\') {
                format!("{}{}", mapping.to, remainder)
            } else {
                format!("{}{}{}", mapping.to, sep, remainder)
            }
        } else {
            path.to_string()
        }
    }

    /// Get lookup statistics for observability.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.lookup_count.load(Ordering::Relaxed),
            self.hit_count.load(Ordering::Relaxed),
        )
    }

    /// Check if the trie is empty (no mappings).
    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty() && self.root.mappings.is_empty()
    }
}

pub mod aider;
pub mod amp;
pub mod chatgpt;
pub mod claude_code;
pub mod cline;
pub mod codex;
pub mod cursor;
pub mod factory;
pub mod gemini;
pub mod opencode;
pub mod pi_agent;

/// High-level detection status for a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionResult {
    pub detected: bool,
    pub evidence: Vec<String>,
    pub root_paths: Vec<PathBuf>,
}

impl DetectionResult {
    pub fn not_found() -> Self {
        Self {
            detected: false,
            evidence: Vec::new(),
            root_paths: Vec::new(),
        }
    }
}

/// A root directory to scan with associated provenance.
///
/// Part of P2.1 - multi-root support for remote sources.
#[derive(Debug)]
pub struct ScanRoot {
    /// Path to scan (e.g., ~/.claude, or /data/remotes/work-laptop/mirror/home/.claude)
    pub path: PathBuf,

    /// Provenance for conversations found under this root.
    /// Injected into every conversation scanned from this root.
    pub origin: Origin,

    /// Optional platform hint (affects path interpretation for workspace mapping).
    pub platform: Option<Platform>,

    /// Optional path rewrite rules.
    /// Used to map remote workspace paths to local equivalents.
    /// Applied at ingest time so filters work across sources.
    pub workspace_rewrites: Vec<PathMapping>,

    /// Cached trie for fast workspace rewriting (Opt 1.5).
    /// Lazily initialized on first use of rewrite_workspace.
    rewrite_trie: OnceCell<Arc<PathTrie>>,
}

impl Clone for ScanRoot {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            origin: self.origin.clone(),
            platform: self.platform,
            workspace_rewrites: self.workspace_rewrites.clone(),
            // Each clone gets its own lazy trie cell
            rewrite_trie: OnceCell::new(),
        }
    }
}

impl ScanRoot {
    /// Create a local scan root with default provenance.
    pub fn local(path: PathBuf) -> Self {
        Self {
            path,
            origin: Origin::local(),
            platform: None,
            workspace_rewrites: Vec::new(),
            rewrite_trie: OnceCell::new(),
        }
    }

    /// Create a remote scan root.
    pub fn remote(path: PathBuf, origin: Origin, platform: Option<Platform>) -> Self {
        Self {
            path,
            origin,
            platform,
            workspace_rewrites: Vec::new(),
            rewrite_trie: OnceCell::new(),
        }
    }

    /// Add a workspace rewrite rule.
    pub fn with_rewrite(
        mut self,
        src_prefix: impl Into<String>,
        dst_prefix: impl Into<String>,
    ) -> Self {
        self.workspace_rewrites
            .push(PathMapping::new(src_prefix, dst_prefix));
        // Invalidate cached trie since mappings changed
        self.rewrite_trie = OnceCell::new();
        self
    }

    /// Get or build the cached rewrite trie.
    fn get_trie(&self) -> &Arc<PathTrie> {
        self.rewrite_trie
            .get_or_init(|| Arc::new(PathTrie::from_mappings(&self.workspace_rewrites)))
    }

    /// Apply workspace rewriting rules to a path.
    ///
    /// Uses a prefix trie for O(k) lookup where k is path depth (Opt 1.5),
    /// instead of O(n) where n is the number of mappings.
    /// Uses longest-prefix matching for correct handling of nested paths.
    /// Optionally filters by agent name.
    pub fn rewrite_workspace(&self, path: &str, agent: Option<&str>) -> String {
        // Fast path: no rewrites configured
        if self.workspace_rewrites.is_empty() {
            return path.to_string();
        }

        // Use cached trie for efficient lookup
        let trie = self.get_trie();
        trie.lookup(path, agent)
    }

    /// Apply workspace rewriting using linear search (original algorithm).
    ///
    /// Kept for benchmarking comparison. Use `rewrite_workspace` for production.
    #[allow(dead_code)]
    pub fn rewrite_workspace_linear(&self, path: &str, agent: Option<&str>) -> String {
        // Sort by prefix length descending for longest-prefix match
        let mut mappings: Vec<_> = self
            .workspace_rewrites
            .iter()
            .filter(|m| m.applies_to_agent(agent))
            .collect();
        mappings.sort_by(|a, b| b.from.len().cmp(&a.from.len()));

        for mapping in mappings {
            if let Some(rewritten) = mapping.apply(path) {
                return rewritten;
            }
        }

        path.to_string()
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

/// Re-assign sequential indices to messages starting from 0.
/// Use this after filtering or sorting messages to ensure idx values are contiguous.
#[inline]
pub fn reindex_messages(messages: &mut [NormalizedMessage]) {
    for (i, msg) in messages.iter_mut().enumerate() {
        msg.idx = i as i64;
    }
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
        // Heuristic:
        // - Values in the typical Unix-seconds range (>= 1e9 and < 1e10) are treated as seconds
        //   and converted to millis.
        // - Negative values are treated as millis (ambiguous, but preserves pre-1970 ms inputs).
        let ts = if (1_000_000_000..10_000_000_000).contains(&ts) {
            ts.saturating_mul(1000)
        } else {
            ts
        };
        return Some(ts);
    }
    // Try ISO-8601 string (modern format)
    if let Some(s) = val.as_str() {
        // Numeric strings (seconds or milliseconds)
        if let Ok(num) = s.parse::<i64>() {
            let ts = if (1_000_000_000..10_000_000_000).contains(&num) {
                num.saturating_mul(1000)
            } else {
                num
            };
            return Some(ts);
        }
        if let Ok(num) = s.parse::<f64>() {
            let ts = if (1_000_000_000.0..10_000_000_000.0).contains(&num) {
                (num * 1000.0).round() as i64
            } else {
                num.round() as i64
            };
            return Some(ts);
        }
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
    // Use single String with push_str to avoid intermediate Vec allocation
    if let Some(arr) = val.as_array() {
        let mut result = String::new();
        for item in arr {
            let part = extract_content_part(item);
            if let Some(text) = part {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str(&text);
            }
        }
        return result;
    }

    String::new()
}

/// Extract text content from a single content block item.
/// Returns None if the item doesn't contain extractable text.
fn extract_content_part(item: &serde_json::Value) -> Option<String> {
    // Handle plain strings in array (e.g., ["Hello", "World"])
    if let Some(text) = item.as_str() {
        return Some(text.to_string());
    }

    let item_type = item.get("type").and_then(|v| v.as_str());

    // Standard text block: {"type": "text", "text": "..."}
    if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
        // Only include if it's a text type or has no type (plain text)
        if item_type.is_none() || item_type == Some("text") || item_type == Some("input_text") {
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
        let root =
            ScanRoot::local(PathBuf::from("/test")).with_rewrite("/home/user", "/Users/local");
        assert_eq!(root.workspace_rewrites.len(), 1);
        assert_eq!(root.workspace_rewrites[0].from, "/home/user");
        assert_eq!(root.workspace_rewrites[0].to, "/Users/local");
    }

    #[test]
    fn scan_root_rewrite_workspace_applies_rules() {
        let root = ScanRoot::local(PathBuf::from("/test"))
            .with_rewrite("/home/user/projects", "/Users/me/projects")
            .with_rewrite("/home/user", "/Users/me");

        // Longest prefix match
        assert_eq!(
            root.rewrite_workspace("/home/user/projects/myapp", None),
            "/Users/me/projects/myapp"
        );

        // Shorter prefix match
        assert_eq!(
            root.rewrite_workspace("/home/user/other", None),
            "/Users/me/other"
        );

        // No match
        assert_eq!(root.rewrite_workspace("/opt/data", None), "/opt/data");
    }

    #[test]
    fn scan_root_rewrite_with_agent_filter() {
        let mut root = ScanRoot::local(PathBuf::from("/test"));
        root.workspace_rewrites
            .push(PathMapping::new("/home/user", "/Users/me"));
        root.workspace_rewrites.push(PathMapping::with_agents(
            "/home/user/projects",
            "/Volumes/Work",
            vec!["claude-code".into()],
        ));

        // With claude-code agent, uses agent-specific mapping
        assert_eq!(
            root.rewrite_workspace("/home/user/projects/app", Some("claude-code")),
            "/Volumes/Work/app"
        );

        // With other agent, falls back to general mapping
        assert_eq!(
            root.rewrite_workspace("/home/user/projects/app", Some("cursor")),
            "/Users/me/projects/app"
        );
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

    // =========================================================================
    // Timestamp parsing edge cases (bead yln.4)
    // =========================================================================

    #[test]
    fn parse_timestamp_i64_milliseconds() {
        let val = serde_json::json!(1700000000000_i64);
        assert_eq!(super::parse_timestamp(&val), Some(1700000000000));
    }

    #[test]
    fn parse_timestamp_i64_seconds() {
        let val = serde_json::json!(1700000000_i64);
        assert_eq!(super::parse_timestamp(&val), Some(1_700_000_000_000));
    }

    #[test]
    fn parse_timestamp_numeric_string_seconds() {
        let val = serde_json::json!("1700000000");
        let result = super::parse_timestamp(&val);
        assert_eq!(result, Some(1_700_000_000_000));
    }

    #[test]
    fn parse_timestamp_numeric_string_millis() {
        let val = serde_json::json!("1700000000000");
        let result = super::parse_timestamp(&val);
        assert_eq!(result, Some(1_700_000_000_000));
    }

    #[test]
    fn parse_timestamp_iso8601_with_fractional() {
        let val = serde_json::json!("2025-11-12T18:31:32.217Z");
        let result = super::parse_timestamp(&val);
        assert!(result.is_some());
        // Should be in milliseconds range for 2025
        assert!(result.unwrap() > 1700000000000);
    }

    #[test]
    fn parse_timestamp_iso8601_without_fractional() {
        let val = serde_json::json!("2025-11-12T18:31:32Z");
        let result = super::parse_timestamp(&val);
        assert!(result.is_some());
        assert!(result.unwrap() > 1700000000000);
    }

    #[test]
    fn parse_timestamp_rfc3339_with_offset() {
        let val = serde_json::json!("2025-11-12T18:31:32+00:00");
        let result = super::parse_timestamp(&val);
        assert!(result.is_some());
    }

    #[test]
    fn parse_timestamp_null_returns_none() {
        let val = serde_json::json!(null);
        assert_eq!(super::parse_timestamp(&val), None);
    }

    #[test]
    fn parse_timestamp_invalid_string_returns_none() {
        let val = serde_json::json!("not a timestamp");
        assert_eq!(super::parse_timestamp(&val), None);
    }

    #[test]
    fn parse_timestamp_empty_string_returns_none() {
        let val = serde_json::json!("");
        assert_eq!(super::parse_timestamp(&val), None);
    }

    #[test]
    fn parse_timestamp_object_returns_none() {
        let val = serde_json::json!({"timestamp": 1234});
        assert_eq!(super::parse_timestamp(&val), None);
    }

    #[test]
    fn parse_timestamp_negative_i64() {
        // Negative timestamps (before 1970) should still work
        let val = serde_json::json!(-1000_i64);
        assert_eq!(super::parse_timestamp(&val), Some(-1000));
    }

    #[test]
    fn parse_timestamp_zero() {
        let val = serde_json::json!(0_i64);
        assert_eq!(super::parse_timestamp(&val), Some(0));
    }

    // =========================================================================
    // Content flattening edge cases (bead yln.4)
    // =========================================================================

    #[test]
    fn flatten_content_plain_string() {
        let val = serde_json::json!("Hello world");
        assert_eq!(super::flatten_content(&val), "Hello world");
    }

    #[test]
    fn flatten_content_text_block_array() {
        let val = serde_json::json!([
            {"type": "text", "text": "Part 1"},
            {"type": "text", "text": "Part 2"}
        ]);
        let result = super::flatten_content(&val);
        assert!(result.contains("Part 1"));
        assert!(result.contains("Part 2"));
    }

    #[test]
    fn flatten_content_tool_use_block() {
        let val = serde_json::json!([
            {"type": "tool_use", "name": "Read", "input": {"path": "/test"}}
        ]);
        let result = super::flatten_content(&val);
        assert!(result.contains("Read"));
    }

    #[test]
    fn flatten_content_mixed_blocks() {
        let val = serde_json::json!([
            {"type": "text", "text": "I'll read the file"},
            {"type": "tool_use", "name": "Read", "input": {"path": "/test"}}
        ]);
        let result = super::flatten_content(&val);
        assert!(result.contains("I'll read the file"));
        assert!(result.contains("Read"));
    }

    #[test]
    fn flatten_content_input_text_block() {
        let val = serde_json::json!([
            {"type": "input_text", "text": "User input here"}
        ]);
        assert!(super::flatten_content(&val).contains("User input here"));
    }

    #[test]
    fn flatten_content_null_returns_empty() {
        let val = serde_json::json!(null);
        assert!(super::flatten_content(&val).is_empty());
    }

    #[test]
    fn flatten_content_empty_array() {
        let val = serde_json::json!([]);
        assert!(super::flatten_content(&val).is_empty());
    }

    #[test]
    fn flatten_content_plain_string_array() {
        // Handle arrays of plain strings (e.g., ["Hello", "World"])
        let val = serde_json::json!(["Hello", "World"]);
        let result = super::flatten_content(&val);
        assert!(result.contains("Hello"));
        assert!(result.contains("World"));
    }

    #[test]
    fn flatten_content_empty_string() {
        let val = serde_json::json!("");
        assert!(super::flatten_content(&val).is_empty());
    }

    #[test]
    fn flatten_content_number_returns_empty() {
        let val = serde_json::json!(42);
        assert!(super::flatten_content(&val).is_empty());
    }

    #[test]
    fn flatten_content_whitespace_only() {
        let val = serde_json::json!("   \n\t  ");
        assert_eq!(super::flatten_content(&val), "   \n\t  ");
    }

    // =========================================================================
    // NormalizedMessage construction (bead yln.4)
    // =========================================================================

    #[test]
    fn normalized_message_default_fields() {
        let msg = NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: None,
            content: "test".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
        };
        assert_eq!(msg.role, "user");
        assert!(msg.author.is_none());
        assert!(msg.created_at.is_none());
    }

    #[test]
    fn normalized_message_with_all_fields() {
        let msg = NormalizedMessage {
            idx: 5,
            role: "assistant".into(),
            author: Some("claude".into()),
            created_at: Some(1700000000000),
            content: "Response text".into(),
            extra: serde_json::json!({"model": "claude-3"}),
            snippets: vec![NormalizedSnippet {
                file_path: Some("test.rs".into()),
                start_line: Some(10),
                end_line: Some(20),
                language: Some("rust".into()),
                snippet_text: Some("fn test()".into()),
            }],
        };
        assert_eq!(msg.idx, 5);
        assert_eq!(msg.author, Some("claude".into()));
        assert_eq!(msg.snippets.len(), 1);
    }

    // =========================================================================
    // NormalizedConversation construction (bead yln.4)
    // =========================================================================

    #[test]
    fn normalized_conversation_minimal() {
        let conv = NormalizedConversation {
            agent_slug: "test_agent".into(),
            external_id: None,
            title: None,
            workspace: None,
            source_path: PathBuf::from("/test/session.jsonl"),
            started_at: None,
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![],
        };
        assert_eq!(conv.agent_slug, "test_agent");
        assert!(conv.messages.is_empty());
    }

    #[test]
    fn normalized_conversation_with_messages() {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some("session-123".into()),
            title: Some("Test session".into()),
            workspace: Some(PathBuf::from("/home/user/project")),
            source_path: PathBuf::from("/data/session.jsonl"),
            started_at: Some(1700000000000),
            ended_at: Some(1700000060000),
            metadata: serde_json::json!({"tokens": 500}),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".into(),
                    author: None,
                    created_at: Some(1700000000000),
                    content: "Hello".into(),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".into(),
                    author: None,
                    created_at: Some(1700000010000),
                    content: "Hi there".into(),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                },
            ],
        };
        assert_eq!(conv.messages.len(), 2);
        assert_eq!(conv.workspace, Some(PathBuf::from("/home/user/project")));
    }

    // =========================================================================
    // DetectionResult (bead yln.4)
    // =========================================================================

    #[test]
    fn detection_result_not_found() {
        let result = DetectionResult::not_found();
        assert!(!result.detected);
        assert!(result.evidence.is_empty());
    }

    #[test]
    fn detection_result_found() {
        let result = DetectionResult {
            detected: true,
            evidence: vec!["found ~/.codex".into(), "has sessions/".into()],
            root_paths: vec![],
        };
        assert!(result.detected);
        assert_eq!(result.evidence.len(), 2);
    }

    // =========================================================================
    // PathTrie (Opt 1.5)
    // =========================================================================

    #[test]
    fn path_trie_empty_lookup() {
        let trie = PathTrie::new();
        assert_eq!(
            trie.lookup("/home/user/project", None),
            "/home/user/project"
        );
    }

    #[test]
    fn path_trie_simple_rewrite() {
        let mut trie = PathTrie::new();
        trie.insert("/home/user", "/Users/me", None);

        assert_eq!(trie.lookup("/home/user/project", None), "/Users/me/project");
    }

    #[test]
    fn path_trie_exact_match() {
        let mut trie = PathTrie::new();
        trie.insert("/home/user", "/Users/me", None);

        assert_eq!(trie.lookup("/home/user", None), "/Users/me");
    }

    #[test]
    fn path_trie_no_match() {
        let mut trie = PathTrie::new();
        trie.insert("/home/user", "/Users/me", None);

        // Different prefix - should not match
        assert_eq!(trie.lookup("/var/log/app", None), "/var/log/app");
    }

    #[test]
    fn path_trie_longest_prefix_match() {
        let mut trie = PathTrie::new();
        trie.insert("/home", "/Users", None);
        trie.insert("/home/user", "/Users/me", None);
        trie.insert("/home/user/projects", "/work", None);

        // Should match /home/user/projects (longest)
        assert_eq!(
            trie.lookup("/home/user/projects/cass/src", None),
            "/work/cass/src"
        );

        // Should match /home/user
        assert_eq!(
            trie.lookup("/home/user/documents", None),
            "/Users/me/documents"
        );

        // Should match /home
        assert_eq!(trie.lookup("/home/other", None), "/Users/other");
    }

    #[test]
    fn path_trie_agent_filter() {
        let mut trie = PathTrie::new();
        trie.insert(
            "/remote/projects",
            "/local/work",
            Some(vec!["claude-code".into()]),
        );
        trie.insert(
            "/remote/projects",
            "/local/other",
            Some(vec!["cursor".into()]),
        );

        // Should use claude-code mapping
        assert_eq!(
            trie.lookup("/remote/projects/app", Some("claude-code")),
            "/local/work/app"
        );

        // Should use cursor mapping
        assert_eq!(
            trie.lookup("/remote/projects/app", Some("cursor")),
            "/local/other/app"
        );

        // No agent filter - uses first matching (agent=None matches all)
        let result = trie.lookup("/remote/projects/app", None);
        assert!(result.starts_with("/local/"));
    }

    #[test]
    fn path_trie_windows_paths() {
        let mut trie = PathTrie::new();
        trie.insert("C:\\Users\\dev", "/home/dev", None);

        assert_eq!(
            trie.lookup("C:\\Users\\dev\\project\\src", None),
            "/home/dev\\project\\src" // Preserves original separator for remainder
        );
    }

    #[test]
    fn path_trie_stats() {
        let mut trie = PathTrie::new();
        trie.insert("/home/user", "/Users/me", None);

        // Initial stats should be 0
        assert_eq!(trie.stats(), (0, 0));

        // Lookup that hits
        let _ = trie.lookup("/home/user/project", None);
        assert_eq!(trie.stats(), (1, 1));

        // Lookup that misses
        let _ = trie.lookup("/var/log", None);
        assert_eq!(trie.stats(), (2, 1));
    }

    #[test]
    fn path_trie_from_mappings() {
        use crate::sources::config::PathMapping;

        let mappings = vec![
            PathMapping::new("/home/user", "/Users/me"),
            PathMapping::new("/opt/app", "/Applications/app"),
        ];

        let trie = PathTrie::from_mappings(&mappings);

        assert_eq!(trie.lookup("/home/user/project", None), "/Users/me/project");
        assert_eq!(trie.lookup("/opt/app/bin", None), "/Applications/app/bin");
    }

    #[test]
    fn scan_root_rewrite_uses_trie() {
        let root = ScanRoot::local(PathBuf::from("/test"))
            .with_rewrite("/home/user", "/Users/me")
            .with_rewrite("/home/user/projects", "/work");

        // Should use longest-prefix match via trie
        assert_eq!(
            root.rewrite_workspace("/home/user/projects/cass", None),
            "/work/cass"
        );

        // Should match shorter prefix
        assert_eq!(
            root.rewrite_workspace("/home/user/documents", None),
            "/Users/me/documents"
        );
    }

    #[test]
    fn scan_root_rewrite_empty() {
        let root = ScanRoot::local(PathBuf::from("/test"));

        // No rewrites - should return original path
        assert_eq!(
            root.rewrite_workspace("/home/user/project", None),
            "/home/user/project"
        );
    }

    #[test]
    fn scan_root_trie_vs_linear_equivalence() {
        let root = ScanRoot::local(PathBuf::from("/test"))
            .with_rewrite("/a/b/c", "/x/y/z")
            .with_rewrite("/a/b", "/x/y")
            .with_rewrite("/a", "/x");

        let paths = ["/a/b/c/d/e", "/a/b/foo", "/a/bar", "/other/path"];

        for path in paths {
            assert_eq!(
                root.rewrite_workspace(path, None),
                root.rewrite_workspace_linear(path, None),
                "Mismatch for path: {}",
                path
            );
        }
    }
}
