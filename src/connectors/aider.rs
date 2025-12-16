use super::{Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext};
use anyhow::Result;
use serde_json::json;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

pub struct AiderConnector;

impl AiderConnector {
    pub fn new() -> Self {
        Self
    }

    /// Find aider chat history files under the provided roots (limited depth to avoid wide scans).
    fn find_chat_files(roots: &[&Path]) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            for entry in WalkDir::new(root)
                .max_depth(5)
                .into_iter()
                .flatten()
                .filter(|e| e.file_type().is_file())
            {
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n == ".aider.chat.history.md")
                {
                    files.push(entry.path().to_path_buf());
                }
            }
        }
        files
    }

    fn parse_chat_history(&self, path: &Path) -> Result<NormalizedConversation> {
        let content = fs::read_to_string(path)?;
        let mut messages = Vec::new();
        let mut current_role = "system";
        let mut current_content = String::new();
        let mut msg_idx = 0;

        for line in content.lines() {
            if line.trim().starts_with("> ") {
                // Only push previous content if switching from non-user role
                if current_role != "user" && !current_content.trim().is_empty() {
                    messages.push(NormalizedMessage {
                        idx: msg_idx,
                        role: current_role.to_string(),
                        author: Some(current_role.to_string()),
                        created_at: None,
                        content: current_content.trim().to_string(),
                        extra: json!({}),
                        snippets: Vec::new(),
                    });
                    msg_idx += 1;
                    current_content.clear();
                }
                current_role = "user";
                current_content.push_str(line.trim_start_matches("> ").trim());
                current_content.push('\n');
            } else {
                if current_role == "user" && !line.trim().is_empty() && !line.starts_with('>') {
                    if !current_content.trim().is_empty() {
                        messages.push(NormalizedMessage {
                            idx: msg_idx,
                            role: "user".to_string(),
                            author: Some("user".to_string()),
                            created_at: None,
                            content: current_content.trim().to_string(),
                            extra: json!({}),
                            snippets: Vec::new(),
                        });
                        msg_idx += 1;
                        current_content.clear();
                    }
                    current_role = "assistant";
                }
                current_content.push_str(line);
                current_content.push('\n');
            }
        }

        if !current_content.trim().is_empty() {
            messages.push(NormalizedMessage {
                idx: msg_idx,
                role: current_role.to_string(),
                author: Some(current_role.to_string()),
                created_at: None,
                content: current_content.trim().to_string(),
                extra: json!({}),
                snippets: Vec::new(),
            });
        }

        let mtime = fs::metadata(path)?.modified()?;
        let ts = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Ok(NormalizedConversation {
            agent_slug: "aider".to_string(),
            external_id: Some(path.file_name().unwrap().to_string_lossy().to_string()),
            title: Some(format!("Aider Chat: {}", path.display())),
            workspace: path.parent().map(std::path::Path::to_path_buf),
            source_path: path.to_path_buf(),
            started_at: Some(ts),
            ended_at: Some(ts),
            metadata: json!({}),
            messages,
        })
    }
}

impl Connector for AiderConnector {
    fn detect(&self) -> DetectionResult {
        // Fast detection: only check for .aider.chat.history.md in CWD (no recursive scan).
        // The expensive WalkDir scan is deferred to scan() where it's actually needed.
        // Also check for CASS_AIDER_DATA_ROOT env var as a signal.
        let cwd = std::env::current_dir().unwrap_or_default();
        let cwd_history = cwd.join(".aider.chat.history.md");

        if cwd_history.exists() {
            return DetectionResult {
                detected: true,
                evidence: vec![format!("found {}", cwd_history.display())],
            };
        }

        if let Some(override_root) = std::env::var_os("CASS_AIDER_DATA_ROOT") {
            let override_path = std::path::PathBuf::from(&override_root);
            let override_history = override_path.join(".aider.chat.history.md");
            if override_history.exists() {
                return DetectionResult {
                    detected: true,
                    evidence: vec![format!("found {}", override_history.display())],
                };
            }
            // Even if file not found, user explicitly set the env var
            return DetectionResult {
                detected: true,
                evidence: vec![format!(
                    "CASS_AIDER_DATA_ROOT set to {}",
                    override_path.display()
                )],
            };
        }

        DetectionResult {
            detected: false,
            evidence: vec![],
        }
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        let files = Self::find_chat_files(std::slice::from_ref(&ctx.data_dir.as_path()));

        let mut conversations = Vec::new();
        for path in files {
            if !super::file_modified_since(&path, ctx.since_ts) {
                continue;
            }
            if let Ok(conv) = self.parse_chat_history(&path) {
                conversations.push(conv);
            }
        }
        Ok(conversations)
    }
}

impl Default for AiderConnector {
    fn default() -> Self {
        Self::new()
    }
}
