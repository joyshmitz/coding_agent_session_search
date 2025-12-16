use coding_agent_search::connectors::claude_code::ClaudeCodeConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn claude_parses_project_fixture() {
    // Setup isolated environment with "claude" in path to satisfy detector
    let tmp = tempfile::TempDir::new().unwrap();
    let fixture_src =
        PathBuf::from("tests/fixtures/claude_code_real/projects/-test-project/agent-test123.jsonl");
    let fixture_dest_dir = tmp.path().join("mock-claude/projects/test-project");
    std::fs::create_dir_all(&fixture_dest_dir).unwrap();
    let fixture_dest = fixture_dest_dir.join("agent-test123.jsonl");
    std::fs::copy(&fixture_src, &fixture_dest).expect("copy fixture");

    // Run scan on temp dir
    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: tmp.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert!(!c.title.as_deref().unwrap_or("").is_empty());
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.messages[1].role, "assistant");
    assert!(c.messages[1].content.contains("matrix completion"));

    // Verify metadata extraction
    let meta = &c.metadata;
    assert_eq!(
        meta.get("sessionId").and_then(|v| v.as_str()),
        Some("test-session")
    );
    assert_eq!(meta.get("gitBranch").and_then(|v| v.as_str()), Some("main"));
}

/// Helper to create a Claude-style temp directory
fn create_claude_temp() -> TempDir {
    TempDir::new().unwrap()
}

/// Test JSONL format with user and assistant messages
#[test]
fn claude_connector_parses_jsonl_format() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","cwd":"/workspace","sessionId":"sess-1","gitBranch":"develop","message":{"role":"user","content":"Hello Claude"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","model":"claude-opus-4","content":[{"type":"text","text":"Hello! How can I help?"}]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.agent_slug, "claude_code");
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.workspace, Some(PathBuf::from("/workspace")));

    // Verify session metadata
    assert_eq!(
        c.metadata.get("sessionId").and_then(|v| v.as_str()),
        Some("sess-1")
    );
    assert_eq!(
        c.metadata.get("gitBranch").and_then(|v| v.as_str()),
        Some("develop")
    );
}

/// Test that summary entries are filtered out
#[test]
fn claude_connector_filters_summary_entries() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Question"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"summary","timestamp":"2025-11-12T18:31:30.000Z","summary":"Summary text"}
{"type":"file-history-snapshot","timestamp":"2025-11-12T18:31:35.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Answer"}]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Should only have user and assistant messages
    assert_eq!(c.messages.len(), 2);
    for msg in &c.messages {
        assert!(!msg.content.contains("Summary text"));
    }
}

/// Test model author extraction
#[test]
fn claude_connector_extracts_model_as_author() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Hello"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","model":"claude-sonnet-4","content":[{"type":"text","text":"Hi!"}]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let assistant = &convs[0].messages[1];
    assert_eq!(assistant.author, Some("claude-sonnet-4".to_string()));
}

/// Test `tool_use` blocks are flattened
#[test]
fn claude_connector_flattens_tool_use() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Read the file"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"I'll read it"},{"type":"tool_use","name":"Read","input":{"file_path":"/src/main.rs"}}]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let assistant = &convs[0].messages[1];
    assert!(assistant.content.contains("I'll read it"));
    assert!(assistant.content.contains("[Tool: Read"));
    assert!(assistant.content.contains("/src/main.rs"));
}

/// Test title extraction from first user message
#[test]
fn claude_connector_extracts_title_from_user() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Help me fix the bug\nMore details here"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Sure!"}]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].title, Some("Help me fix the bug".to_string()));
}

/// Test title fallback to workspace name
#[test]
fn claude_connector_title_fallback_to_workspace() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    // Only assistant message, no user
    let sample = r#"{"type":"assistant","cwd":"/home/user/my-project","message":{"role":"assistant","content":[{"type":"text","text":"Starting up"}]},"timestamp":"2025-11-12T18:31:18.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    // Should fallback to workspace directory name
    assert_eq!(convs[0].title, Some("my-project".to_string()));
}

/// Test malformed lines are skipped
#[test]
fn claude_connector_skips_malformed_lines() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Valid"},"timestamp":"2025-11-12T18:31:18.000Z"}
{ not valid json
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Response"}]},"timestamp":"2025-11-12T18:31:20.000Z"}
also not json
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].messages.len(), 2);
}

/// Test empty content is filtered
#[test]
fn claude_connector_filters_empty_content() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"   "},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"user","message":{"role":"user","content":"Valid content"},"timestamp":"2025-11-12T18:31:19.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[]},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    // Only the message with "Valid content" should be included
    assert_eq!(convs[0].messages.len(), 1);
    assert!(convs[0].messages[0].content.contains("Valid content"));
}

/// Test sequential index assignment
#[test]
fn claude_connector_assigns_sequential_indices() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"First"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Second"}]},"timestamp":"2025-11-12T18:31:19.000Z"}
{"type":"user","message":{"role":"user","content":"Third"},"timestamp":"2025-11-12T18:31:20.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.messages.len(), 3);
    assert_eq!(c.messages[0].idx, 0);
    assert_eq!(c.messages[1].idx, 1);
    assert_eq!(c.messages[2].idx, 2);
}

/// Test multiple files in directory
#[test]
fn claude_connector_handles_multiple_files() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();

    for i in 1..=3 {
        let file = projects.join(format!("session-{i}.jsonl"));
        let sample = format!(
            r#"{{"type":"user","message":{{"role":"user","content":"Message {i}"}},"timestamp":"2025-11-12T18:31:18.000Z"}}
"#
        );
        fs::write(&file, sample).unwrap();
    }

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 3);
}

/// Test JSON format (non-JSONL)
#[test]
fn claude_connector_parses_json_format() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("conversation.json");

    let sample = r#"{
        "title": "Test Conversation",
        "messages": [
            {"role": "user", "content": "Hello", "timestamp": "2025-11-12T18:31:18.000Z"},
            {"role": "assistant", "content": "Hi there!", "timestamp": "2025-11-12T18:31:20.000Z"}
        ]
    }"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.title, Some("Test Conversation".to_string()));
    assert_eq!(c.messages.len(), 2);
}

/// Test .claude extension
#[test]
fn claude_connector_parses_claude_extension() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("conversation.claude");

    let sample = r#"{
        "messages": [
            {"role": "user", "content": "Question", "timestamp": "2025-11-12T18:31:18.000Z"},
            {"role": "assistant", "content": "Answer", "timestamp": "2025-11-12T18:31:20.000Z"}
        ]
    }"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].messages.len(), 2);
}

/// Test empty directory returns empty results
#[test]
fn claude_connector_handles_empty_directory() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects");
    fs::create_dir_all(&projects).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert!(convs.is_empty());
}

/// Test `external_id` is filename
#[test]
fn claude_connector_sets_external_id() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("unique-session-id.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Test"},"timestamp":"2025-11-12T18:31:18.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(
        convs[0].external_id,
        Some("unique-session-id.jsonl".to_string())
    );
}

/// Test `source_path` is set correctly
#[test]
fn claude_connector_sets_source_path() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Test"},"timestamp":"2025-11-12T18:31:18.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].source_path, file);
}

/// Test timestamps are parsed correctly
#[test]
fn claude_connector_parses_timestamps() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"First"},"timestamp":"2025-11-12T18:31:18.000Z"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Last"}]},"timestamp":"2025-11-12T18:31:30.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert!(c.started_at.is_some());
    assert!(c.ended_at.is_some());
    // started_at should be earlier than ended_at
    assert!(c.started_at.unwrap() < c.ended_at.unwrap());
}

/// Test long title is truncated
#[test]
fn claude_connector_truncates_long_title() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    let long_text = "A".repeat(200);
    let sample = format!(
        r#"{{"type":"user","message":{{"role":"user","content":"{long_text}"}},"timestamp":"2025-11-12T18:31:18.000Z"}}
"#
    );
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert!(convs[0].title.is_some());
    assert_eq!(convs[0].title.as_ref().unwrap().len(), 100);
}

/// Test non-supported file extensions are ignored
#[test]
fn claude_connector_ignores_other_extensions() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();

    // Valid file
    let valid = projects.join("session.jsonl");
    fs::write(
        &valid,
        r#"{"type":"user","message":{"role":"user","content":"Valid"},"timestamp":"2025-11-12T18:31:18.000Z"}
"#,
    )
    .unwrap();

    // Invalid extensions
    let txt = projects.join("notes.txt");
    let md = projects.join("readme.md");
    let log = projects.join("debug.log");
    fs::write(&txt, "text").unwrap();
    fs::write(&md, "markdown").unwrap();
    fs::write(&log, "logs").unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
}

/// Test nested project directories
#[test]
fn claude_connector_handles_nested_projects() {
    let dir = create_claude_temp();
    let nested = dir.path().join("mock-claude/projects/org/team/project");
    fs::create_dir_all(&nested).unwrap();
    let file = nested.join("session.jsonl");

    let sample = r#"{"type":"user","message":{"role":"user","content":"Nested"},"timestamp":"2025-11-12T18:31:18.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert!(convs[0].source_path.to_string_lossy().contains("team"));
}

/// Test role extraction from entry type when message.role is missing
#[test]
fn claude_connector_uses_entry_type_as_role() {
    let dir = create_claude_temp();
    let projects = dir.path().join("mock-claude/projects/test-proj");
    fs::create_dir_all(&projects).unwrap();
    let file = projects.join("session.jsonl");

    // message without role field, should use type field as role
    let sample = r#"{"type":"user","message":{"content":"No role field"},"timestamp":"2025-11-12T18:31:18.000Z"}
"#;
    fs::write(&file, sample).unwrap();

    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join("mock-claude"),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].messages[0].role, "user");
}
