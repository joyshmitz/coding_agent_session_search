//! Tests for the OpenCode connector (JSON file-based storage)

use coding_agent_search::connectors::opencode::OpenCodeConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper to create a JSON-based OpenCode storage structure
fn create_test_storage(dir: &std::path::Path, sessions: &[TestSession]) -> std::io::Result<()> {
    // Create directories
    fs::create_dir_all(dir.join("session"))?;
    fs::create_dir_all(dir.join("message"))?;
    fs::create_dir_all(dir.join("part"))?;

    for session in sessions {
        // Create project dir
        let project_dir = dir.join("session").join(&session.project_id);
        fs::create_dir_all(&project_dir)?;

        // Write session file
        let session_json = serde_json::json!({
            "id": session.id,
            "title": session.title,
            "directory": session.directory,
            "projectID": session.project_id,
            "time": {
                "created": session.created,
                "updated": session.updated
            }
        });
        fs::write(
            project_dir.join(format!("{}.json", session.id)),
            serde_json::to_string_pretty(&session_json)?,
        )?;

        // Create message directory for this session
        let msg_dir = dir.join("message").join(&session.id);
        fs::create_dir_all(&msg_dir)?;

        for msg in &session.messages {
            // Write message file
            let msg_json = serde_json::json!({
                "id": msg.id,
                "sessionID": session.id,
                "role": msg.role,
                "modelID": msg.model_id,
                "time": {
                    "created": msg.created
                }
            });
            fs::write(
                msg_dir.join(format!("{}.json", msg.id)),
                serde_json::to_string_pretty(&msg_json)?,
            )?;

            // Create part directory and write parts
            let part_dir = dir.join("part").join(&msg.id);
            fs::create_dir_all(&part_dir)?;

            for (i, part) in msg.parts.iter().enumerate() {
                let part_json = serde_json::json!({
                    "id": format!("part{}", i),
                    "messageID": msg.id,
                    "type": part.part_type,
                    "text": part.text,
                    "state": part.state.as_ref().map(|s| serde_json::json!({
                        "output": s
                    }))
                });
                fs::write(
                    part_dir.join(format!("part{}.json", i)),
                    serde_json::to_string_pretty(&part_json)?,
                )?;
            }
        }
    }

    Ok(())
}

struct TestSession {
    id: String,
    project_id: String,
    title: Option<String>,
    directory: Option<String>,
    created: Option<i64>,
    updated: Option<i64>,
    messages: Vec<TestMessage>,
}

struct TestMessage {
    id: String,
    role: String,
    model_id: Option<String>,
    created: Option<i64>,
    parts: Vec<TestPart>,
}

struct TestPart {
    part_type: String,
    text: Option<String>,
    state: Option<String>,
}

#[test]
fn opencode_parses_json_fixture() {
    let fixture_root = PathBuf::from("tests/fixtures/opencode_json");
    let conn = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: fixture_root.clone(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.title.as_deref(), Some("OpenCode JSON Session"));
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.workspace, Some(PathBuf::from("/tmp/test-project")));
}

#[test]
fn opencode_parses_created_storage() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "test-session-1".into(),
            project_id: "proj1".into(),
            title: Some("My Test Session".into()),
            directory: Some("/home/user/project".into()),
            created: Some(1000),
            updated: Some(5000),
            messages: vec![
                TestMessage {
                    id: "msg1".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(1000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("Hello world".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "msg2".into(),
                    role: "assistant".into(),
                    model_id: Some("claude-3".into()),
                    created: Some(2000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("Hi there!".into()),
                        state: None,
                    }],
                },
            ],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.title, Some("My Test Session".to_string()));
    assert_eq!(c.workspace, Some(PathBuf::from("/home/user/project")));
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.messages[0].content, "Hello world");
    assert_eq!(c.messages[1].content, "Hi there!");
    assert_eq!(c.messages[1].author, Some("claude-3".to_string()));
}

#[test]
fn opencode_handles_multiple_sessions() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[
            TestSession {
                id: "session-a".into(),
                project_id: "proj1".into(),
                title: Some("Session A".into()),
                directory: None,
                created: Some(1000),
                updated: None,
                messages: vec![TestMessage {
                    id: "msg-a1".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(1000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("Message A".into()),
                        state: None,
                    }],
                }],
            },
            TestSession {
                id: "session-b".into(),
                project_id: "proj2".into(),
                title: Some("Session B".into()),
                directory: None,
                created: Some(2000),
                updated: None,
                messages: vec![TestMessage {
                    id: "msg-b1".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(2000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("Message B".into()),
                        state: None,
                    }],
                }],
            },
        ],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 2);

    let titles: Vec<_> = convs.iter().filter_map(|c| c.title.as_deref()).collect();
    assert!(titles.contains(&"Session A"));
    assert!(titles.contains(&"Session B"));
}

#[test]
fn opencode_handles_tool_parts() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "tool-session".into(),
            project_id: "proj1".into(),
            title: Some("Tool Session".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "tool-msg".into(),
                role: "assistant".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![
                    TestPart {
                        part_type: "text".into(),
                        text: Some("Let me check that file.".into()),
                        state: None,
                    },
                    TestPart {
                        part_type: "tool".into(),
                        text: None,
                        state: Some("file contents here".into()),
                    },
                ],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let content = &convs[0].messages[0].content;
    assert!(content.contains("Let me check that file."));
    assert!(content.contains("[Tool Output]"));
    assert!(content.contains("file contents here"));
}

#[test]
fn opencode_handles_reasoning_parts() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "reasoning-session".into(),
            project_id: "proj1".into(),
            title: Some("Reasoning Session".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "reasoning-msg".into(),
                role: "assistant".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![
                    TestPart {
                        part_type: "reasoning".into(),
                        text: Some("I need to think about this...".into()),
                        state: None,
                    },
                    TestPart {
                        part_type: "text".into(),
                        text: Some("The answer is 42.".into()),
                        state: None,
                    },
                ],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let content = &convs[0].messages[0].content;
    assert!(content.contains("[Reasoning]"));
    assert!(content.contains("I need to think about this..."));
    assert!(content.contains("The answer is 42."));
}

#[test]
fn opencode_sets_correct_agent_slug() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "slug-test".into(),
            project_id: "proj1".into(),
            title: Some("Test".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "msg".into(),
                role: "user".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![TestPart {
                    part_type: "text".into(),
                    text: Some("test".into()),
                    state: None,
                }],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].agent_slug, "opencode");
}

#[test]
fn opencode_handles_empty_storage() {
    let dir = TempDir::new().unwrap();
    fs::create_dir_all(dir.path().join("session")).unwrap();
    fs::create_dir_all(dir.path().join("message")).unwrap();
    fs::create_dir_all(dir.path().join("part")).unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert!(convs.is_empty());
}

#[test]
fn opencode_handles_missing_storage() {
    let dir = TempDir::new().unwrap();
    // Don't create any directories

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert!(convs.is_empty());
}

#[test]
fn opencode_orders_messages_by_timestamp() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "order-session".into(),
            project_id: "proj1".into(),
            title: Some("Order Test".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![
                TestMessage {
                    id: "msg-late".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(3000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("third".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "msg-early".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(1000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("first".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "msg-middle".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(2000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("second".into()),
                        state: None,
                    }],
                },
            ],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let msgs = &convs[0].messages;
    assert_eq!(msgs[0].content, "first");
    assert_eq!(msgs[1].content, "second");
    assert_eq!(msgs[2].content, "third");
}

#[test]
fn opencode_assigns_sequential_indices() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "idx-session".into(),
            project_id: "proj1".into(),
            title: Some("Index Test".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![
                TestMessage {
                    id: "m0".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(1000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("first".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "m1".into(),
                    role: "assistant".into(),
                    model_id: None,
                    created: Some(2000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("second".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "m2".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(3000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("third".into()),
                        state: None,
                    }],
                },
            ],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let msgs = &convs[0].messages;
    for (i, msg) in msgs.iter().enumerate() {
        assert_eq!(msg.idx, i as i64);
    }
}

#[test]
fn opencode_title_fallback_to_first_message() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "no-title".into(),
            project_id: "proj1".into(),
            title: None, // No title
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "msg".into(),
                role: "user".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![TestPart {
                    part_type: "text".into(),
                    text: Some("This is the first message content".into()),
                    state: None,
                }],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    // Title should fall back to first line of first message
    assert_eq!(
        convs[0].title,
        Some("This is the first message content".to_string())
    );
}

#[test]
fn opencode_computes_started_ended_at() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "time-session".into(),
            project_id: "proj1".into(),
            title: Some("Time Test".into()),
            directory: None,
            created: Some(500),  // Session created at 500
            updated: Some(4000), // Session updated at 4000
            messages: vec![
                TestMessage {
                    id: "m0".into(),
                    role: "user".into(),
                    model_id: None,
                    created: Some(1000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("first".into()),
                        state: None,
                    }],
                },
                TestMessage {
                    id: "m1".into(),
                    role: "assistant".into(),
                    model_id: None,
                    created: Some(3000),
                    parts: vec![TestPart {
                        part_type: "text".into(),
                        text: Some("last".into()),
                        state: None,
                    }],
                },
            ],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    // started_at comes from session time.created
    assert_eq!(convs[0].started_at, Some(500));
    // ended_at comes from session time.updated
    assert_eq!(convs[0].ended_at, Some(4000));
}

#[test]
fn opencode_skips_sessions_without_messages() {
    let dir = TempDir::new().unwrap();

    // Create session dir but no messages
    let project_dir = dir.path().join("session").join("proj1");
    fs::create_dir_all(&project_dir).unwrap();

    let session_json = serde_json::json!({
        "id": "empty-session",
        "title": "Empty Session",
        "projectID": "proj1"
    });
    fs::write(
        project_dir.join("empty-session.json"),
        serde_json::to_string_pretty(&session_json).unwrap(),
    )
    .unwrap();

    // Create empty message directory
    fs::create_dir_all(dir.path().join("message").join("empty-session")).unwrap();
    fs::create_dir_all(dir.path().join("part")).unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();

    // Should skip sessions without messages
    assert!(convs.is_empty());
}

#[test]
fn opencode_metadata_contains_session_id() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "meta-test".into(),
            project_id: "proj1".into(),
            title: Some("Meta Test".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "msg".into(),
                role: "user".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![TestPart {
                    part_type: "text".into(),
                    text: Some("test".into()),
                    state: None,
                }],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let metadata = &convs[0].metadata;
    assert_eq!(
        metadata.get("session_id").and_then(|v| v.as_str()),
        Some("meta-test")
    );
    assert_eq!(
        metadata.get("project_id").and_then(|v| v.as_str()),
        Some("proj1")
    );
}

#[test]
fn opencode_external_id_is_session_id() {
    let dir = TempDir::new().unwrap();
    create_test_storage(
        dir.path(),
        &[TestSession {
            id: "external-id-test".into(),
            project_id: "proj1".into(),
            title: Some("External ID Test".into()),
            directory: None,
            created: Some(1000),
            updated: None,
            messages: vec![TestMessage {
                id: "msg".into(),
                role: "user".into(),
                model_id: None,
                created: Some(1000),
                parts: vec![TestPart {
                    part_type: "text".into(),
                    text: Some("test".into()),
                    state: None,
                }],
            }],
        }],
    )
    .unwrap();

    let connector = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().to_path_buf(),
        scan_roots: Vec::new(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    assert_eq!(convs[0].external_id.as_deref(), Some("external-id-test"));
}
