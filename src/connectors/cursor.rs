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
