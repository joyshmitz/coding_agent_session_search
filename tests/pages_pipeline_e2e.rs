use assert_cmd::Command;
use coding_agent_search::connectors::NormalizedConversation;
use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::pages::bundle::BundleBuilder;
use coding_agent_search::pages::encrypt::EncryptionEngine;
use coding_agent_search::pages::export::{ExportEngine, ExportFilter, PathMode};
use coding_agent_search::storage::sqlite::SqliteStorage;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[path = "util/mod.rs"]
mod util;

use util::ConversationFixtureBuilder;

#[test]
fn test_pages_export_pipeline_e2e() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");

    fs::create_dir_all(&data_dir).unwrap();

    // 1. Setup: Create fixtures and Populate DB
    setup_db(&data_dir);

    // 2. Export (Simulating `cass pages --export-only`)
    // Create unfiltered export of the database
    let export_staging = temp_dir.path().join("export_staging");
    fs::create_dir_all(&export_staging).unwrap();
    let export_db_path = export_staging.join("export.db");
    let source_db_path = data_dir.join("agent_search.db");

    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };

    let export_engine = ExportEngine::new(&source_db_path, &export_db_path, filter);
    let stats = export_engine
        .execute(|_, _| {}, None)
        .expect("ExportEngine execution failed");

    assert_eq!(
        stats.conversations_processed, 1,
        "Should export 1 conversation"
    );
    assert!(export_db_path.exists(), "Export database should exist");

    // 3. Encrypt (Simulating Wizard/Encrypt Step)
    let encrypt_staging = temp_dir.path().join("encrypt_staging");
    let mut enc_engine = EncryptionEngine::new(1024 * 1024); // 1MB chunks
    enc_engine
        .add_password_slot("test-password")
        .expect("Failed to add password slot");
    enc_engine
        .add_recovery_slot(b"recovery-secret-bytes")
        .expect("Failed to add recovery slot");

    let _enc_config = enc_engine
        .encrypt_file(&export_db_path, &encrypt_staging, |_, _| {})
        .expect("Encryption failed");

    assert!(encrypt_staging.join("config.json").exists());
    assert!(encrypt_staging.join("payload").exists());

    // 4. Bundle (Simulating Bundle Step)
    let bundle_dir = temp_dir.path().join("bundle");
    let builder = BundleBuilder::new()
        .title("E2E Test Archive")
        .description("Test archive for E2E pipeline")
        .generate_qr(false) // Skip QR generation to avoid dependency issues if any
        .recovery_secret(Some(b"recovery-secret-bytes".to_vec()));

    let bundle_result = builder
        .build(&encrypt_staging, &bundle_dir, |_, _| {})
        .expect("Bundle failed");

    assert!(bundle_result.site_dir.join("index.html").exists());
    assert!(
        bundle_result
            .private_dir
            .join("recovery-secret.txt")
            .exists()
    );

    // 5. Verify (CLI)
    // Run `cass pages --verify <site_dir>` to validate the bundle integrity and structure
    let site_dir = bundle_dir.join("site");
    let mut cmd = Command::cargo_bin("cass").unwrap();
    let assert = cmd
        .arg("pages")
        .arg("--verify")
        .arg(&site_dir)
        .arg("--json")
        .assert();

    assert.success();
}

#[test]
#[ignore] // TODO: Debug why cass output is empty in test environment
fn test_secret_scan_gating() {
    let temp_dir = TempDir::new().unwrap();

    // Setup XDG_DATA_HOME structure
    let xdg_data_home = temp_dir.path().join("xdg_data");
    let cass_data_dir = xdg_data_home.join("coding-agent-search");
    fs::create_dir_all(&cass_data_dir).unwrap();

    setup_db_with_secret(&cass_data_dir);

    // 1. Scan secrets (report only)
    let mut cmd = Command::cargo_bin("cass").unwrap();
    let output = cmd
        .env("XDG_DATA_HOME", &xdg_data_home)
        .arg("pages")
        .arg("--scan-secrets")
        .arg("--json")
        .output()
        .unwrap();

    if !output.status.success() {
        eprintln!("Stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    assert!(
        output.status.success(),
        "Scan should succeed in report mode"
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("valid json output");

    let findings = json.get("findings").expect("findings field in output");
    let findings_array = findings.as_array().expect("findings should be array");
    assert!(!findings_array.is_empty(), "Should detect inserted secret");

    // Check that we found the specific type of secret
    let found_api_key = findings_array.iter().any(|f| {
        f.get("kind")
            .and_then(|k| k.as_str())
            .map(|s| s.contains("API Key") || s.contains("OpenAI"))
            .unwrap_or(false)
    });
    assert!(found_api_key, "Should detect the fake API key");

    // 2. Fail on secrets
    let mut cmd_fail = Command::cargo_bin("cass").unwrap();
    cmd_fail
        .env("XDG_DATA_HOME", &xdg_data_home)
        .arg("pages")
        .arg("--scan-secrets")
        .arg("--fail-on-secrets")
        .assert()
        .failure(); // Should exit with non-zero code
}

fn setup_db(data_dir: &Path) {
    setup_db_internal(data_dir, false);
}

fn setup_db_with_secret(data_dir: &Path) {
    setup_db_internal(data_dir, true);
}

fn setup_db_internal(data_dir: &Path, include_secret: bool) {
    let db_path = data_dir.join("agent_search.db");
    if let Some(p) = db_path.parent() {
        fs::create_dir_all(p).unwrap();
    }

    // Initialize DB with schema
    let mut storage = SqliteStorage::open(&db_path).expect("Failed to open storage");

    // Create Agent
    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // Create Workspace
    let workspace_path = Path::new("/home/user/projects/test");
    let workspace_id = Some(
        storage
            .ensure_workspace(workspace_path, None)
            .expect("ensure workspace"),
    );

    let content = if include_secret {
        "I accidentally pasted my key: sk-proj-1234567890abcdef1234567890abcdef1234567890abcdef"
    } else {
        "Agent response 1"
    };

    // Create a fixture conversation
    let conversation = ConversationFixtureBuilder::new("claude_code")
        .title("Test Conversation")
        .workspace(workspace_path)
        .source_path("/home/user/.claude/projects/test/session.jsonl")
        .messages(5)
        .with_content(0, "User message 1")
        .with_content(1, content)
        .build_conversation();

    // Insert into DB
    storage
        .insert_conversation_tree(agent_id, workspace_id, &conversation)
        .expect("Failed to insert conversation");
}
