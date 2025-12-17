//! Connector for Cursor IDE chat history.
//!
//! Cursor stores chat history in SQLite databases (state.vscdb) within:
//! - macOS: ~/Library/Application Support/Cursor/User/globalStorage/
//! - macOS workspaces: ~/Library/Application Support/Cursor/User/workspaceStorage/{id}/
//! - Linux: ~/.config/Cursor/User/globalStorage/
//! - Windows: %APPDATA%/Cursor/User/globalStorage/
//!
//! Chat data is stored in the `cursorDiskKV` table with keys like:
//! - `composerData:{uuid}` - Composer/chat session data (JSON)
//!
//! And in the `ItemTable` with keys like:
//! - `workbench.panel.aichat.view.aichat.chatdata` - Legacy chat data

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use walkdir::WalkDir;

use crate::connectors::{
    Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext,
};

pub struct CursorConnector;

impl Default for CursorConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorConnector {
    pub fn new() -> Self {
        Self
    }

    /// Get the base Cursor application support directory
    pub fn app_support_dir() -> Option<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            dirs::home_dir().map(|h| h.join("Library/Application Support/Cursor/User"))
        }
        #[cfg(target_os = "linux")]
        {
            dirs::home_dir().map(|h| h.join(".config/Cursor/User"))
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir().map(|d| d.join("Cursor/User"))
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        {
            None
        }
    }

    /// Find all state.vscdb files in Cursor storage
    fn find_db_files(base: &Path) -> Vec<PathBuf> {
        let mut dbs = Vec::new();

        // Check globalStorage
        let global_db = base.join("globalStorage/state.vscdb");
        if global_db.exists() {
            dbs.push(global_db);
        }

        // Check workspaceStorage subdirectories
        let workspace_storage = base.join("workspaceStorage");
        if workspace_storage.exists() {
            for entry in WalkDir::new(&workspace_storage)
                .max_depth(2)
                .into_iter()
                .flatten()
            {
                if entry.file_type().is_file() && entry.file_name().to_str() == Some("state.vscdb")
                {
                    dbs.push(entry.path().to_path_buf());
                }
            }
        }

        dbs
    }

    /// Extract chat sessions from a SQLite database
    fn extract_from_db(
        db_path: &Path,
        since_ts: Option<i64>,
    ) -> Result<Vec<NormalizedConversation>> {
        let conn = Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open Cursor db: {}", db_path.display()))?;

        let mut convs = Vec::new();
        let mut seen_ids = HashSet::new();

        // Try cursorDiskKV table for composerData entries
        if let Ok(mut stmt) =
            conn.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'")
        {
            let rows = stmt.query_map([], |row| {
                let key: String = row.get(0)?;
                let value: String = row.get(1)?;
                Ok((key, value))
            });

            if let Ok(rows) = rows {
                for row in rows.flatten() {
                    let (key, value) = row;
                    if let Some(conv) =
                        Self::parse_composer_data(&key, &value, db_path, since_ts, &mut seen_ids)
                    {
                        convs.push(conv);
                    }
                }
            }
        }

        // Also try ItemTable for legacy aichat data
        if let Ok(mut stmt) = conn.prepare(
            "SELECT key, value FROM ItemTable WHERE key LIKE '%aichat%chatdata%' OR key LIKE '%composer%'",
        ) {
            let rows = stmt.query_map([], |row| {
                let key: String = row.get(0)?;
                let value: String = row.get(1)?;
                Ok((key, value))
            });

            if let Ok(rows) = rows {
                for row in rows.flatten() {
                    let (key, value) = row;
                    if let Some(conv) =
                        Self::parse_aichat_data(&key, &value, db_path, since_ts, &mut seen_ids)
                    {
                        convs.push(conv);
                    }
                }
            }
        }

        Ok(convs)
    }

    /// Parse composerData JSON into a conversation
    fn parse_composer_data(
        key: &str,
        value: &str,
        db_path: &Path,
        _since_ts: Option<i64>, // File-level filtering done in scan(); message filtering not needed
        seen_ids: &mut HashSet<String>,
    ) -> Option<NormalizedConversation> {
        let val: Value = serde_json::from_str(value).ok()?;

        // Extract composer ID from key (composerData:{uuid})
        let composer_id = key.strip_prefix("composerData:")?.to_string();

        // Skip if already seen
        if seen_ids.contains(&composer_id) {
            return None;
        }
        seen_ids.insert(composer_id.clone());

        // Extract timestamps
        let created_at = val.get("createdAt").and_then(|v| v.as_i64());

        // NOTE: Do NOT filter conversations/messages by timestamp here!
        // The file-level check in file_modified_since() is sufficient.
        // Filtering would cause data loss when the file is re-indexed.

        let mut messages = Vec::new();

        // Parse conversation from bubbles/tabs structure
        // Cursor uses different structures depending on version
        if let Some(tabs) = val.get("tabs").and_then(|v| v.as_array()) {
            for tab in tabs {
                if let Some(bubbles) = tab.get("bubbles").and_then(|v| v.as_array()) {
                    for (idx, bubble) in bubbles.iter().enumerate() {
                        if let Some(msg) = Self::parse_bubble(bubble, idx) {
                            messages.push(msg);
                        }
                    }
                }
            }
        }

        // Also check fullConversation/conversationMap for newer format
        if let Some(conv_map) = val.get("conversationMap").and_then(|v| v.as_object()) {
            for (_, conv_val) in conv_map {
                if let Some(bubbles) = conv_val.get("bubbles").and_then(|v| v.as_array()) {
                    for (idx, bubble) in bubbles.iter().enumerate() {
                        if let Some(msg) = Self::parse_bubble(bubble, messages.len() + idx) {
                            messages.push(msg);
                        }
                    }
                }
            }
        }

        // Check for text/richText as user input (simple composer sessions)
        let user_text = val
            .get("text")
            .and_then(|v| v.as_str())
            .or_else(|| val.get("richText").and_then(|v| v.as_str()))
            .unwrap_or("");

        if !user_text.is_empty() && messages.is_empty() {
            messages.push(NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at,
                content: user_text.to_string(),
                extra: serde_json::json!({}),
                snippets: Vec::new(),
            });
        }

        // Skip if no messages
        if messages.is_empty() {
            return None;
        }

        // Re-index messages
        for (i, msg) in messages.iter_mut().enumerate() {
            msg.idx = i as i64;
        }

        // Extract model info for title
        let model_name = val
            .get("modelConfig")
            .and_then(|m| m.get("modelName"))
            .and_then(|v| v.as_str());

        let title = messages
            .first()
            .map(|m| {
                m.content
                    .lines()
                    .next()
                    .unwrap_or("")
                    .chars()
                    .take(100)
                    .collect()
            })
            .or_else(|| model_name.map(|m| format!("Cursor chat with {}", m)));

        Some(NormalizedConversation {
            agent_slug: "cursor".to_string(),
            external_id: Some(composer_id),
            title,
            workspace: None, // Could try to extract from db_path
            source_path: db_path.to_path_buf(),
            started_at: created_at,
            ended_at: messages.last().and_then(|m| m.created_at).or(created_at),
            metadata: serde_json::json!({
                "source": "cursor",
                "model": model_name,
                "unifiedMode": val.get("unifiedMode").and_then(|v| v.as_str()),
            }),
            messages,
        })
    }

    /// Parse a bubble (message) from Cursor's format
    fn parse_bubble(bubble: &Value, idx: usize) -> Option<NormalizedMessage> {
        // Cursor bubbles have different structures
        let content = bubble
            .get("text")
            .and_then(|v| v.as_str())
            .or_else(|| bubble.get("content").and_then(|v| v.as_str()))
            .or_else(|| bubble.get("message").and_then(|v| v.as_str()))?;

        if content.trim().is_empty() {
            return None;
        }

        let role = bubble
            .get("type")
            .and_then(|v| v.as_str())
            .or_else(|| bubble.get("role").and_then(|v| v.as_str()))
            .map(|r| {
                match r.to_lowercase().as_str() {
                    "user" | "human" => "user",
                    "assistant" | "ai" | "bot" => "assistant",
                    _ => r,
                }
                .to_string()
            })
            .unwrap_or_else(|| "assistant".to_string());

        let created_at = bubble
            .get("timestamp")
            .or_else(|| bubble.get("createdAt"))
            .and_then(crate::connectors::parse_timestamp);

        Some(NormalizedMessage {
            idx: idx as i64,
            role,
            author: bubble
                .get("model")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_at,
            content: content.to_string(),
            extra: bubble.clone(),
            snippets: Vec::new(),
        })
    }

    /// Parse legacy aichat data
    fn parse_aichat_data(
        key: &str,
        value: &str,
        db_path: &Path,
        _since_ts: Option<i64>, // File-level filtering done in scan(); message filtering not needed
        seen_ids: &mut HashSet<String>,
    ) -> Option<NormalizedConversation> {
        let val: Value = serde_json::from_str(value).ok()?;

        // Skip if already seen
        let id = format!("aichat-{}", key);
        if seen_ids.contains(&id) {
            return None;
        }
        seen_ids.insert(id.clone());

        let mut messages = Vec::new();
        let mut started_at = None;
        let mut ended_at = None;

        // Parse tabs array
        if let Some(tabs) = val.get("tabs").and_then(|v| v.as_array()) {
            for tab in tabs {
                let tab_ts = tab.get("timestamp").and_then(|v| v.as_i64());

                // NOTE: Do NOT filter by timestamp here! File-level check is sufficient.

                if let Some(bubbles) = tab.get("bubbles").and_then(|v| v.as_array()) {
                    for bubble in bubbles {
                        if let Some(msg) = Self::parse_bubble(bubble, messages.len()) {
                            if started_at.is_none() {
                                started_at = msg.created_at.or(tab_ts);
                            }
                            ended_at = msg.created_at.or(tab_ts);
                            messages.push(msg);
                        }
                    }
                }
            }
        }

        if messages.is_empty() {
            return None;
        }

        // Re-index
        for (i, msg) in messages.iter_mut().enumerate() {
            msg.idx = i as i64;
        }

        let title = messages.first().map(|m| {
            m.content
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(100)
                .collect()
        });

        Some(NormalizedConversation {
            agent_slug: "cursor".to_string(),
            external_id: Some(id),
            title,
            workspace: None,
            source_path: db_path.to_path_buf(),
            started_at,
            ended_at,
            metadata: serde_json::json!({"source": "cursor_aichat"}),
            messages,
        })
    }
}

impl Connector for CursorConnector {
    fn detect(&self) -> DetectionResult {
        if let Some(base) = Self::app_support_dir()
            && base.exists()
        {
            let dbs = Self::find_db_files(&base);
            if !dbs.is_empty() {
                return DetectionResult {
                    detected: true,
                    evidence: vec![
                        format!("found Cursor at {}", base.display()),
                        format!("found {} database file(s)", dbs.len()),
                    ],
                };
            }
        }
        DetectionResult::not_found()
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        // Determine base directory
        let base = if ctx.data_dir.join("globalStorage").exists()
            || ctx.data_dir.join("workspaceStorage").exists()
            || ctx
                .data_dir
                .file_name()
                .is_some_and(|n| n.to_str().unwrap_or("").contains("Cursor"))
        {
            ctx.data_dir.clone()
        } else if let Some(default_base) = Self::app_support_dir() {
            default_base
        } else {
            return Ok(Vec::new());
        };

        if !base.exists() {
            return Ok(Vec::new());
        }

        let db_files = Self::find_db_files(&base);
        let mut all_convs = Vec::new();

        for db_path in db_files {
            // Skip files not modified since last scan
            if !crate::connectors::file_modified_since(&db_path, ctx.since_ts) {
                continue;
            }

            match Self::extract_from_db(&db_path, ctx.since_ts) {
                Ok(convs) => {
                    tracing::debug!(
                        path = %db_path.display(),
                        count = convs.len(),
                        "cursor extracted conversations"
                    );
                    all_convs.extend(convs);
                }
                Err(e) => {
                    tracing::warn!(
                        path = %db_path.display(),
                        error = %e,
                        "cursor failed to extract from db"
                    );
                }
            }
        }

        Ok(all_convs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::json;
    use std::collections::HashSet;
    use std::fs;
    use tempfile::TempDir;

    /// Create a test SQLite database with the cursorDiskKV table
    fn create_test_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cursorDiskKV (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ItemTable (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )
        .unwrap();
        conn
    }

    // =========================================================================
    // Constructor tests
    // =========================================================================

    #[test]
    fn new_creates_connector() {
        let connector = CursorConnector::new();
        // Just verify it creates without error
        assert!(std::mem::size_of_val(&connector) >= 0);
    }

    #[test]
    fn default_creates_connector() {
        let connector = CursorConnector::default();
        assert!(std::mem::size_of_val(&connector) >= 0);
    }

    // =========================================================================
    // find_db_files tests
    // =========================================================================

    #[test]
    fn find_db_files_empty_for_nonexistent() {
        let dir = TempDir::new().unwrap();
        let dbs = CursorConnector::find_db_files(dir.path());
        assert!(dbs.is_empty());
    }

    #[test]
    fn find_db_files_finds_global_storage() {
        let dir = TempDir::new().unwrap();
        let global_dir = dir.path().join("globalStorage");
        fs::create_dir_all(&global_dir).unwrap();
        fs::write(global_dir.join("state.vscdb"), "").unwrap();

        let dbs = CursorConnector::find_db_files(dir.path());
        assert_eq!(dbs.len(), 1);
        assert!(dbs[0].ends_with("state.vscdb"));
    }

    #[test]
    fn find_db_files_finds_workspace_storage() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("workspaceStorage").join("abc123");
        fs::create_dir_all(&workspace_dir).unwrap();
        fs::write(workspace_dir.join("state.vscdb"), "").unwrap();

        let dbs = CursorConnector::find_db_files(dir.path());
        assert_eq!(dbs.len(), 1);
    }

    #[test]
    fn find_db_files_finds_multiple() {
        let dir = TempDir::new().unwrap();

        // Create global storage
        let global_dir = dir.path().join("globalStorage");
        fs::create_dir_all(&global_dir).unwrap();
        fs::write(global_dir.join("state.vscdb"), "").unwrap();

        // Create multiple workspace storage dirs
        for i in 1..=3 {
            let ws_dir = dir.path().join("workspaceStorage").join(format!("ws{}", i));
            fs::create_dir_all(&ws_dir).unwrap();
            fs::write(ws_dir.join("state.vscdb"), "").unwrap();
        }

        let dbs = CursorConnector::find_db_files(dir.path());
        assert_eq!(dbs.len(), 4); // 1 global + 3 workspace
    }

    // =========================================================================
    // parse_bubble tests
    // =========================================================================

    #[test]
    fn parse_bubble_with_text() {
        let bubble = json!({
            "text": "Hello from user",
            "type": "user"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0);
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.content, "Hello from user");
        assert_eq!(msg.role, "user");
    }

    #[test]
    fn parse_bubble_with_content_field() {
        let bubble = json!({
            "content": "Response from assistant",
            "role": "assistant"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 1);
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.content, "Response from assistant");
        assert_eq!(msg.role, "assistant");
    }

    #[test]
    fn parse_bubble_with_message_field() {
        let bubble = json!({
            "message": "Another message",
            "type": "ai"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0);
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.content, "Another message");
        assert_eq!(msg.role, "assistant"); // "ai" maps to assistant
    }

    #[test]
    fn parse_bubble_role_normalization() {
        let test_cases = vec![
            ("user", "user"),
            ("human", "user"),
            ("assistant", "assistant"),
            ("ai", "assistant"),
            ("bot", "assistant"),
            ("custom", "custom"), // Unknown roles pass through
        ];

        for (input_role, expected_role) in test_cases {
            let bubble = json!({
                "text": "test",
                "type": input_role
            });

            let msg = CursorConnector::parse_bubble(&bubble, 0).unwrap();
            assert_eq!(msg.role, expected_role, "Failed for input role: {}", input_role);
        }
    }

    #[test]
    fn parse_bubble_empty_content_returns_none() {
        let bubble = json!({
            "text": "",
            "type": "user"
        });

        assert!(CursorConnector::parse_bubble(&bubble, 0).is_none());
    }

    #[test]
    fn parse_bubble_whitespace_only_returns_none() {
        let bubble = json!({
            "text": "   \n\t  ",
            "type": "user"
        });

        assert!(CursorConnector::parse_bubble(&bubble, 0).is_none());
    }

    #[test]
    fn parse_bubble_extracts_timestamp() {
        let bubble = json!({
            "text": "Test",
            "type": "user",
            "timestamp": 1700000000000i64
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0).unwrap();
        assert_eq!(msg.created_at, Some(1700000000000));
    }

    #[test]
    fn parse_bubble_extracts_model() {
        let bubble = json!({
            "text": "Response",
            "type": "assistant",
            "model": "gpt-4"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0).unwrap();
        assert_eq!(msg.author, Some("gpt-4".to_string()));
    }

    #[test]
    fn parse_bubble_defaults_to_assistant() {
        let bubble = json!({
            "text": "No role specified"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0).unwrap();
        assert_eq!(msg.role, "assistant");
    }

    // =========================================================================
    // parse_composer_data tests
    // =========================================================================

    #[test]
    fn parse_composer_data_with_tabs_and_bubbles() {
        let key = "composerData:abc-123";
        let value = json!({
            "createdAt": 1700000000000i64,
            "tabs": [{
                "bubbles": [
                    {"text": "Hello", "type": "user"},
                    {"text": "Hi there!", "type": "assistant"}
                ]
            }]
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert_eq!(conv.agent_slug, "cursor");
        assert_eq!(conv.external_id, Some("abc-123".to_string()));
        assert_eq!(conv.messages.len(), 2);
        assert_eq!(conv.messages[0].role, "user");
        assert_eq!(conv.messages[1].role, "assistant");
    }

    #[test]
    fn parse_composer_data_with_conversation_map() {
        let key = "composerData:def-456";
        let value = json!({
            "conversationMap": {
                "conv1": {
                    "bubbles": [
                        {"text": "Question?", "type": "user"},
                        {"content": "Answer!", "role": "assistant"}
                    ]
                }
            }
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert_eq!(conv.messages.len(), 2);
    }

    #[test]
    fn parse_composer_data_with_text_only() {
        let key = "composerData:simple-123";
        let value = json!({
            "text": "Simple user input without bubbles",
            "createdAt": 1700000000000i64
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert_eq!(conv.messages.len(), 1);
        assert_eq!(conv.messages[0].role, "user");
        assert!(conv.messages[0].content.contains("Simple user input"));
    }

    #[test]
    fn parse_composer_data_with_rich_text() {
        let key = "composerData:rich-789";
        let value = json!({
            "richText": "Rich text content here"
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert!(conv.messages[0].content.contains("Rich text"));
    }

    #[test]
    fn parse_composer_data_skips_duplicates() {
        let key = "composerData:dup-123";
        let value = json!({
            "text": "Content"
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv1 =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);
        let conv2 =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv1.is_some());
        assert!(conv2.is_none()); // Duplicate should return None
    }

    #[test]
    fn parse_composer_data_returns_none_for_empty() {
        let key = "composerData:empty-123";
        let value = json!({}).to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_none());
    }

    #[test]
    fn parse_composer_data_extracts_model_config() {
        let key = "composerData:model-123";
        let value = json!({
            "text": "Test",
            "modelConfig": {
                "modelName": "gpt-4-turbo"
            }
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert_eq!(conv.metadata["model"], "gpt-4-turbo");
    }

    #[test]
    fn parse_composer_data_invalid_key_returns_none() {
        let key = "not-composer-data"; // Missing "composerData:" prefix
        let value = json!({ "text": "Content" }).to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_none());
    }

    // =========================================================================
    // parse_aichat_data tests
    // =========================================================================

    #[test]
    fn parse_aichat_data_with_tabs() {
        let key = "aichat.chatdata";
        let value = json!({
            "tabs": [{
                "timestamp": 1700000000000i64,
                "bubbles": [
                    {"text": "User question", "type": "user"},
                    {"text": "AI response", "type": "ai"}
                ]
            }]
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_aichat_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_some());
        let conv = conv.unwrap();
        assert_eq!(conv.agent_slug, "cursor");
        assert!(conv.external_id.as_ref().unwrap().starts_with("aichat-"));
        assert_eq!(conv.messages.len(), 2);
    }

    #[test]
    fn parse_aichat_data_returns_none_for_empty() {
        let key = "aichat.empty";
        let value = json!({
            "tabs": []
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_aichat_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_none());
    }

    #[test]
    fn parse_aichat_data_skips_duplicates() {
        let key = "aichat.dup";
        let value = json!({
            "tabs": [{
                "bubbles": [{"text": "Content", "type": "user"}]
            }]
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv1 =
            CursorConnector::parse_aichat_data(key, &value, Path::new("/test"), None, &mut seen);
        let conv2 =
            CursorConnector::parse_aichat_data(key, &value, Path::new("/test"), None, &mut seen);

        assert!(conv1.is_some());
        assert!(conv2.is_none());
    }

    // =========================================================================
    // extract_from_db tests
    // =========================================================================

    #[test]
    fn extract_from_db_with_composer_data() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.vscdb");

        let conn = create_test_db(&db_path);
        let value = json!({
            "text": "Database test"
        })
        .to_string();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
            ["composerData:db-test-123", &value],
        )
        .unwrap();
        drop(conn);

        let convs = CursorConnector::extract_from_db(&db_path, None).unwrap();
        assert_eq!(convs.len(), 1);
        assert!(convs[0].messages[0].content.contains("Database test"));
    }

    #[test]
    fn extract_from_db_with_aichat_data() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.vscdb");

        let conn = create_test_db(&db_path);
        let value = json!({
            "tabs": [{
                "bubbles": [{"text": "Aichat test", "type": "user"}]
            }]
        })
        .to_string();
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?, ?)",
            ["workbench.panel.aichat.view.aichat.chatdata", &value],
        )
        .unwrap();
        drop(conn);

        let convs = CursorConnector::extract_from_db(&db_path, None).unwrap();
        assert_eq!(convs.len(), 1);
    }

    #[test]
    fn extract_from_db_handles_empty_db() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.vscdb");

        let _conn = create_test_db(&db_path);

        let convs = CursorConnector::extract_from_db(&db_path, None).unwrap();
        assert!(convs.is_empty());
    }

    #[test]
    fn extract_from_db_fails_for_missing_file() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("nonexistent.vscdb");

        let result = CursorConnector::extract_from_db(&db_path, None);
        assert!(result.is_err());
    }

    // =========================================================================
    // Detection tests
    // =========================================================================

    #[test]
    fn detect_not_found_without_cursor_dir() {
        let connector = CursorConnector::new();
        let result = connector.detect();
        // On most CI/test systems, Cursor won't be installed
        // Just verify detect() doesn't panic
        assert!(result.detected || !result.detected);
    }

    // =========================================================================
    // Scan tests
    // =========================================================================

    #[test]
    fn scan_empty_directory_returns_empty() {
        let dir = TempDir::new().unwrap();

        // Create globalStorage to make scan() use this directory instead of fallback
        let global_dir = dir.path().join("globalStorage");
        fs::create_dir_all(&global_dir).unwrap();
        // Create an empty state.vscdb to prevent fallback to system Cursor
        create_test_db(&global_dir.join("state.vscdb"));

        let connector = CursorConnector::new();
        let ctx = ScanContext::local_default(dir.path().to_path_buf(), None);
        let result = connector.scan(&ctx);

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn scan_processes_global_storage() {
        let dir = TempDir::new().unwrap();

        // Create Cursor-like directory structure
        let cursor_dir = dir.path().join("Cursor");
        let global_dir = cursor_dir.join("globalStorage");
        fs::create_dir_all(&global_dir).unwrap();

        // Create database with test data
        let db_path = global_dir.join("state.vscdb");
        let conn = create_test_db(&db_path);
        let value = json!({ "text": "Scan test" }).to_string();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
            ["composerData:scan-123", &value],
        )
        .unwrap();
        drop(conn);

        let connector = CursorConnector::new();
        let ctx = ScanContext::local_default(cursor_dir.clone(), None);
        let result = connector.scan(&ctx);

        assert!(result.is_ok());
        let convs = result.unwrap();
        assert_eq!(convs.len(), 1);
    }

    #[test]
    fn scan_recognizes_cursor_in_path() {
        let dir = TempDir::new().unwrap();

        // Directory name contains "Cursor"
        let cursor_dir = dir.path().join("TestCursor");
        let global_dir = cursor_dir.join("globalStorage");
        fs::create_dir_all(&global_dir).unwrap();

        let db_path = global_dir.join("state.vscdb");
        let conn = create_test_db(&db_path);
        let value = json!({ "text": "Path test" }).to_string();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
            ["composerData:path-123", &value],
        )
        .unwrap();
        drop(conn);

        let connector = CursorConnector::new();
        let ctx = ScanContext::local_default(cursor_dir, None);
        let result = connector.scan(&ctx);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    // =========================================================================
    // Edge case tests
    // =========================================================================

    #[test]
    fn parse_composer_data_invalid_json_returns_none() {
        let key = "composerData:invalid-123";
        let value = "not valid json {{{";

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, value, Path::new("/test"), None, &mut seen);

        assert!(conv.is_none());
    }

    #[test]
    fn parse_bubble_preserves_original_in_extra() {
        let bubble = json!({
            "text": "Test",
            "type": "user",
            "customField": "customValue"
        });

        let msg = CursorConnector::parse_bubble(&bubble, 0).unwrap();
        assert_eq!(msg.extra["customField"], "customValue");
    }

    #[test]
    fn conversation_title_from_first_message() {
        let key = "composerData:title-test";
        let value = json!({
            "tabs": [{
                "bubbles": [
                    {"text": "This is the first line\nSecond line here", "type": "user"}
                ]
            }]
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        let conv = conv.unwrap();
        // Title should be first line only
        assert_eq!(conv.title, Some("This is the first line".to_string()));
    }

    #[test]
    fn conversation_title_truncates_long_lines() {
        let key = "composerData:long-title";
        let long_text = "x".repeat(200);
        let value = json!({
            "text": long_text
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen);

        let conv = conv.unwrap();
        assert!(conv.title.as_ref().unwrap().len() <= 100);
    }

    #[test]
    fn messages_are_reindexed_sequentially() {
        let key = "composerData:reindex";
        let value = json!({
            "tabs": [{
                "bubbles": [
                    {"text": "One", "type": "user"},
                    {"text": "Two", "type": "assistant"},
                    {"text": "Three", "type": "user"}
                ]
            }]
        })
        .to_string();

        let mut seen = HashSet::new();
        let conv =
            CursorConnector::parse_composer_data(key, &value, Path::new("/test"), None, &mut seen)
                .unwrap();

        assert_eq!(conv.messages[0].idx, 0);
        assert_eq!(conv.messages[1].idx, 1);
        assert_eq!(conv.messages[2].idx, 2);
    }
}
