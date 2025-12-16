//! OpenCode connector for JSON file-based storage.
//!
//! OpenCode stores data at `~/.local/share/opencode/storage/` using a hierarchical
//! JSON file structure:
//!   - session/{projectID}/{sessionID}.json  - Session metadata
//!   - message/{sessionID}/{messageID}.json  - Message metadata
//!   - part/{messageID}/{partID}.json        - Actual message content

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use crate::connectors::{
    Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext,
};

pub struct OpenCodeConnector;

impl Default for OpenCodeConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenCodeConnector {
    pub fn new() -> Self {
        Self
    }

    /// Get the OpenCode storage directory.
    /// OpenCode stores sessions in ~/.local/share/opencode/storage/
    fn storage_root() -> Option<PathBuf> {
        // Check for env override first (useful for testing)
        if let Ok(path) = std::env::var("OPENCODE_STORAGE_ROOT") {
            let p = PathBuf::from(path);
            if p.exists() {
                return Some(p);
            }
        }

        // Primary location: XDG data directory (Linux/macOS)
        if let Some(data) = dirs::data_local_dir() {
            let storage_dir = data.join("opencode/storage");
            if storage_dir.exists() {
                return Some(storage_dir);
            }
        }

        // Fallback: ~/.local/share/opencode/storage
        if let Some(home) = dirs::home_dir() {
            let storage_dir = home.join(".local/share/opencode/storage");
            if storage_dir.exists() {
                return Some(storage_dir);
            }
        }

        None
    }
}

// ============================================================================
// JSON Structures for OpenCode Storage
// ============================================================================

/// Session info from session/{projectID}/{sessionID}.json
#[derive(Debug, Deserialize)]
struct SessionInfo {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    directory: Option<String>,
    #[serde(rename = "projectID", default)]
    project_id: Option<String>,
    #[serde(default)]
    time: Option<SessionTime>,
}

#[derive(Debug, Deserialize)]
struct SessionTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    updated: Option<i64>,
}

/// Message info from message/{sessionID}/{messageID}.json
#[derive(Debug, Deserialize)]
struct MessageInfo {
    id: String,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    time: Option<MessageTime>,
    #[serde(rename = "modelID", default)]
    model_id: Option<String>,
    #[serde(rename = "sessionID", default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    completed: Option<i64>,
}

/// Part info from part/{messageID}/{partID}.json
#[derive(Debug, Clone, Deserialize)]
struct PartInfo {
    #[serde(default)]
    #[allow(dead_code)]
    id: Option<String>,
    #[serde(rename = "messageID", default)]
    message_id: Option<String>,
    #[serde(rename = "type", default)]
    part_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    // Tool state for tool parts
    #[serde(default)]
    state: Option<ToolState>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolState {
    #[serde(default)]
    output: Option<String>,
}

impl Connector for OpenCodeConnector {
    fn detect(&self) -> DetectionResult {
        if let Some(storage) = Self::storage_root() {
            DetectionResult {
                detected: true,
                evidence: vec![format!("found {}", storage.display())],
            }
        } else {
            DetectionResult::not_found()
        }
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        // Determine the storage root
        let storage_root = if ctx.data_dir.exists() && looks_like_opencode_storage(&ctx.data_dir)
        {
            ctx.data_dir.clone()
        } else {
            match Self::storage_root() {
                Some(root) => root,
                None => return Ok(Vec::new()),
            }
        };

        let session_dir = storage_root.join("session");
        let message_dir = storage_root.join("message");
        let part_dir = storage_root.join("part");

        if !session_dir.exists() {
            return Ok(Vec::new());
        }

        // Collect all session files
        let session_files: Vec<PathBuf> = WalkDir::new(&session_dir)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .map(|e| e.path().to_path_buf())
            .collect();

        let mut convs = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for session_file in session_files {
            // Skip files not modified since last scan
            if !crate::connectors::file_modified_since(&session_file, ctx.since_ts) {
                continue;
            }

            // Parse session
            let session = match parse_session_file(&session_file) {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(
                        "opencode: failed to parse session {}: {e}",
                        session_file.display()
                    );
                    continue;
                }
            };

            // Deduplicate by session ID
            if !seen_ids.insert(session.id.clone()) {
                continue;
            }

            // Load messages for this session
            let session_msg_dir = message_dir.join(&session.id);
            let messages = if session_msg_dir.exists() {
                load_messages(&session_msg_dir, &part_dir)?
            } else {
                Vec::new()
            };

            if messages.is_empty() {
                continue;
            }

            // Build normalized conversation
            let started_at = session
                .time
                .as_ref()
                .and_then(|t| t.created)
                .or_else(|| messages.first().and_then(|m| m.created_at));
            let ended_at = session
                .time
                .as_ref()
                .and_then(|t| t.updated)
                .or_else(|| messages.last().and_then(|m| m.created_at));

            let workspace = session.directory.map(PathBuf::from);
            let title = session.title.or_else(|| {
                messages
                    .first()
                    .and_then(|m| m.content.lines().next())
                    .map(|s| s.chars().take(100).collect())
            });

            convs.push(NormalizedConversation {
                agent_slug: "opencode".into(),
                external_id: Some(session.id.clone()),
                title,
                workspace,
                source_path: session_file.clone(),
                started_at,
                ended_at,
                metadata: serde_json::json!({
                    "session_id": session.id,
                    "project_id": session.project_id,
                }),
                messages,
            });
        }

        Ok(convs)
    }
}

/// Check if a directory looks like OpenCode storage
fn looks_like_opencode_storage(path: &std::path::Path) -> bool {
    let path_str = path.to_string_lossy().to_lowercase();
    path_str.contains("opencode")
        || path.join("session").exists()
        || path.join("message").exists()
        || path.join("part").exists()
}

/// Parse a session JSON file
fn parse_session_file(path: &PathBuf) -> Result<SessionInfo> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read session file {}", path.display()))?;
    let session: SessionInfo = serde_json::from_str(&content)
        .with_context(|| format!("parse session JSON {}", path.display()))?;
    Ok(session)
}

/// Load all messages for a session
fn load_messages(session_msg_dir: &PathBuf, part_dir: &PathBuf) -> Result<Vec<NormalizedMessage>> {
    let mut messages = Vec::new();

    // Find all message files for this session
    let msg_files: Vec<PathBuf> = WalkDir::new(session_msg_dir)
        .max_depth(1)
        .into_iter()
        .flatten()
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect();

    // Build a map of message_id -> parts
    let mut parts_by_msg: HashMap<String, Vec<PartInfo>> = HashMap::new();

    // Scan part directory for all parts
    if part_dir.exists() {
        for entry in WalkDir::new(part_dir).into_iter().flatten() {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false)
                && let Ok(content) = fs::read_to_string(path)
                && let Ok(part) = serde_json::from_str::<PartInfo>(&content)
                && let Some(msg_id) = &part.message_id
            {
                parts_by_msg.entry(msg_id.clone()).or_default().push(part);
            }
        }
    }

    for msg_file in msg_files {
        let content = match fs::read_to_string(&msg_file) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let msg_info: MessageInfo = match serde_json::from_str(&content) {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Get parts for this message
        let parts = parts_by_msg.get(&msg_info.id).cloned().unwrap_or_default();

        // Assemble message content from parts
        let content_text = assemble_content_from_parts(&parts);
        if content_text.trim().is_empty() {
            continue;
        }

        // Determine role
        let role = msg_info
            .role
            .clone()
            .unwrap_or_else(|| "assistant".to_string());

        // Determine timestamp
        let created_at = msg_info.time.as_ref().and_then(|t| t.created);

        // Author from model_id for assistant messages
        let author = if role == "assistant" {
            msg_info.model_id.clone()
        } else {
            Some("user".to_string())
        };

        messages.push(NormalizedMessage {
            idx: 0, // Will be assigned later
            role,
            author,
            created_at,
            content: content_text,
            extra: serde_json::json!({
                "message_id": msg_info.id,
                "session_id": msg_info.session_id,
            }),
            snippets: Vec::new(),
        });
    }

    // Sort by timestamp and assign indices
    messages.sort_by_key(|m| m.created_at.unwrap_or(i64::MAX));
    for (i, msg) in messages.iter_mut().enumerate() {
        msg.idx = i as i64;
    }

    Ok(messages)
}

/// Assemble message content from parts
fn assemble_content_from_parts(parts: &[PartInfo]) -> String {
    let mut content_pieces: Vec<String> = Vec::new();

    for part in parts {
        match part.part_type.as_deref() {
            Some("text") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(text.clone());
                }
            }
            Some("tool") => {
                // Include tool output if available
                if let Some(state) = &part.state
                    && let Some(output) = &state.output
                    && !output.trim().is_empty()
                {
                    content_pieces.push(format!("[Tool Output]\n{}", output));
                }
            }
            Some("reasoning") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(format!("[Reasoning]\n{}", text));
                }
            }
            Some("patch") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(format!("[Patch]\n{}", text));
                }
            }
            // Ignore step-start, step-finish, and other control parts
            _ => {}
        }
    }

    content_pieces.join("\n\n")
}
