//! End-to-end integration tests for the Pages export pipeline (P6.5).
//!
//! This module validates the complete workflow:
//! - Export → Encrypt → Bundle → Verify → Decrypt
//!
//! # Running
//!
//! ```bash
//! # Run all pages E2E tests
//! cargo test --test e2e_pages
//!
//! # Run with detailed logging
//! RUST_LOG=debug cargo test --test e2e_pages -- --nocapture
//!
//! # Run specific test
//! cargo test --test e2e_pages test_full_export_pipeline_password_only
//! ```

use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::pages::bundle::{BundleBuilder, BundleResult};
use coding_agent_search::pages::encrypt::{DecryptionEngine, EncryptionEngine, load_config};
use coding_agent_search::pages::export::{ExportEngine, ExportFilter, PathMode};
use coding_agent_search::pages::verify::verify_bundle;
use coding_agent_search::storage::sqlite::SqliteStorage;
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

#[path = "util/mod.rs"]
mod util;

use util::ConversationFixtureBuilder;

// =============================================================================
// Test Constants
// =============================================================================

const TEST_PASSWORD: &str = "test-password-123!";
const TEST_RECOVERY_SECRET: &[u8] = b"recovery-secret-32bytes-padding!";
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB chunks

// =============================================================================
// Helper Functions
// =============================================================================

/// Log a test phase with timing for CI parsing.
fn log_phase(phase: &str, start: Instant) {
    let duration_ms = start.elapsed().as_millis();
    eprintln!("{{\"phase\":\"{}\",\"duration_ms\":{},\"status\":\"PASS\"}}", phase, duration_ms);
}

/// Setup a test database with conversations.
fn setup_test_db(data_dir: &Path, conversation_count: usize) -> std::path::PathBuf {
    let db_path = data_dir.join("agent_search.db");

    let mut storage = SqliteStorage::open(&db_path).expect("Failed to open storage");

    // Create agent
    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // Create workspace
    let workspace_path = Path::new("/home/user/projects/test");
    let workspace_id = Some(
        storage
            .ensure_workspace(workspace_path, None)
            .expect("ensure workspace"),
    );

    // Create conversations
    for i in 0..conversation_count {
        let conversation = ConversationFixtureBuilder::new("claude_code")
            .title(format!("Test Conversation {}", i))
            .workspace(workspace_path)
            .source_path(format!("/home/user/.claude/projects/test/session-{}.jsonl", i))
            .messages(10)
            .with_content(0, format!("User message {} - requesting help with code", i))
            .with_content(1, format!("Assistant response {} - here's the solution", i))
            .build_conversation();

        storage
            .insert_conversation_tree(agent_id, workspace_id, &conversation)
            .expect("Failed to insert conversation");
    }

    db_path
}

/// Build the complete pipeline and return artifacts.
struct PipelineArtifacts {
    export_db_path: std::path::PathBuf,
    bundle: BundleResult,
    _temp_dir: TempDir, // Keep alive for duration of test
}

fn build_full_pipeline(
    conversation_count: usize,
    include_password: bool,
    include_recovery: bool,
) -> PipelineArtifacts {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("Failed to create data directory");

    // Step 1: Setup database
    let start = Instant::now();
    let source_db_path = setup_test_db(&data_dir, conversation_count);
    log_phase("setup_database", start);

    // Step 2: Export
    let start = Instant::now();
    let export_staging = temp_dir.path().join("export_staging");
    fs::create_dir_all(&export_staging).expect("Failed to create export staging");
    let export_db_path = export_staging.join("export.db");

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
        .expect("Export failed");
    assert!(
        stats.conversations_processed > 0,
        "Should export at least one conversation"
    );
    log_phase("export", start);

    // Step 3: Encrypt
    let start = Instant::now();
    let encrypt_dir = temp_dir.path().join("encrypt_staging");
    let mut enc_engine = EncryptionEngine::new(CHUNK_SIZE);

    if include_password {
        enc_engine
            .add_password_slot(TEST_PASSWORD)
            .expect("Failed to add password slot");
    }

    if include_recovery {
        enc_engine
            .add_recovery_slot(TEST_RECOVERY_SECRET)
            .expect("Failed to add recovery slot");
    }

    let _enc_config = enc_engine
        .encrypt_file(&export_db_path, &encrypt_dir, |_, _| {})
        .expect("Encryption failed");
    log_phase("encrypt", start);

    // Step 4: Bundle
    let start = Instant::now();
    let bundle_dir = temp_dir.path().join("bundle");
    let mut builder = BundleBuilder::new()
        .title("E2E Test Archive")
        .description("Test archive for integration tests")
        .generate_qr(false);

    if include_recovery {
        builder = builder.recovery_secret(Some(TEST_RECOVERY_SECRET.to_vec()));
    }

    let bundle = builder
        .build(&encrypt_dir, &bundle_dir, |_, _| {})
        .expect("Bundle failed");
    log_phase("bundle", start);

    PipelineArtifacts {
        export_db_path,
        bundle,
        _temp_dir: temp_dir,
    }
}

// =============================================================================
// Test: Full Export Pipeline (Password Only)
// =============================================================================

/// Test the complete export pipeline with password-only authentication.
#[test]
fn test_full_export_pipeline_password_only() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_full_export_pipeline_password_only\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(5, true, false);

    // Verify bundle structure
    let site = &artifacts.bundle.site_dir;
    assert!(site.join("index.html").exists(), "index.html should exist");
    assert!(site.join("sw.js").exists(), "sw.js should exist");
    assert!(site.join("config.json").exists(), "config.json should exist");
    assert!(site.join("payload").exists(), "payload directory should exist");

    // Verify config.json has single key slot
    let config_str = fs::read_to_string(site.join("config.json")).expect("read config");
    let config: serde_json::Value = serde_json::from_str(&config_str).expect("parse config");
    let slots = config.get("key_slots").expect("key_slots field");
    assert_eq!(slots.as_array().unwrap().len(), 1, "Should have 1 key slot");
    assert_eq!(
        slots[0].get("kdf").unwrap().as_str().unwrap(),
        "argon2id",
        "Should use argon2id KDF"
    );

    log_phase("verify_structure", start);
    eprintln!("{{\"test\":\"test_full_export_pipeline_password_only\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

// =============================================================================
// Test: Full Export Pipeline (Password + Recovery)
// =============================================================================

/// Test the complete export pipeline with dual authentication (password + recovery).
#[test]
fn test_full_export_pipeline_dual_auth() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_full_export_pipeline_dual_auth\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(3, true, true);

    // Verify config.json has two key slots
    let site = &artifacts.bundle.site_dir;
    let config_str = fs::read_to_string(site.join("config.json")).expect("read config");
    let config: serde_json::Value = serde_json::from_str(&config_str).expect("parse config");
    let slots = config.get("key_slots").expect("key_slots field");
    let slots_arr = slots.as_array().unwrap();
    assert_eq!(slots_arr.len(), 2, "Should have 2 key slots");

    // Verify first slot is password (argon2id)
    assert_eq!(
        slots_arr[0].get("kdf").unwrap().as_str().unwrap(),
        "argon2id"
    );

    // Verify second slot is recovery (hkdf-sha256)
    assert_eq!(
        slots_arr[1].get("kdf").unwrap().as_str().unwrap(),
        "hkdf-sha256"
    );

    // Verify private directory has recovery secret
    assert!(
        artifacts.bundle.private_dir.join("recovery-secret.txt").exists(),
        "recovery-secret.txt should exist"
    );

    eprintln!("{{\"test\":\"test_full_export_pipeline_dual_auth\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

// =============================================================================
// Test: Integrity and Decrypt Roundtrip
// =============================================================================

/// Test that decrypted payload matches original export database.
#[test]
fn test_integrity_decrypt_roundtrip_password() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_password\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(2, true, true);

    // Decrypt with password
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor =
        DecryptionEngine::unlock_with_password(config, TEST_PASSWORD).expect("unlock password");
    let decrypted_path = temp_dir.path().join("decrypted_password.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt with password");

    // Verify bytes match
    let original = fs::read(&artifacts.export_db_path).expect("read original");
    let decrypted = fs::read(&decrypted_path).expect("read decrypted");
    assert_eq!(
        original, decrypted,
        "Decrypted content should match original"
    );

    log_phase("decrypt_password", start);
    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_password\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

/// Test that decrypted payload matches original using recovery key.
#[test]
fn test_integrity_decrypt_roundtrip_recovery() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_recovery\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(2, true, true);

    // Decrypt with recovery key
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor = DecryptionEngine::unlock_with_recovery(config, TEST_RECOVERY_SECRET)
        .expect("unlock recovery");
    let decrypted_path = temp_dir.path().join("decrypted_recovery.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt with recovery");

    // Verify bytes match
    let original = fs::read(&artifacts.export_db_path).expect("read original");
    let decrypted = fs::read(&decrypted_path).expect("read decrypted");
    assert_eq!(
        original, decrypted,
        "Decrypted content should match original"
    );

    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_recovery\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

// =============================================================================
// Test: Tampering Detection
// =============================================================================

/// Test that tampering with a chunk fails authentication.
#[test]
fn test_tampering_fails_authentication() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_tampering_fails_authentication\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(2, true, false);
    let site_dir = &artifacts.bundle.site_dir;

    // Baseline: verify passes
    let baseline = verify_bundle(site_dir, false).expect("verify baseline");
    assert_eq!(baseline.status, "valid", "Baseline should be valid");

    // Find and corrupt a payload chunk
    let payload_dir = site_dir.join("payload");
    let chunk = fs::read_dir(&payload_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().map(|e| e == "bin").unwrap_or(false))
        .expect("payload chunk");
    fs::write(&chunk, b"corrupted payload data").expect("corrupt chunk");

    // Verify should now detect corruption
    let result = verify_bundle(site_dir, false).expect("verify after corruption");
    assert_eq!(result.status, "invalid", "Corrupted bundle should be invalid");

    eprintln!("{{\"test\":\"test_tampering_fails_authentication\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

// =============================================================================
// Test: Bundle Verification
// =============================================================================

/// Test CLI verify command works correctly.
/// NOTE: Requires the cass binary to be built first (`cargo build`)
#[test]
#[ignore = "Requires cass binary - run with --ignored or after cargo build"]
fn test_cli_verify_command() {
    use assert_cmd::cargo::cargo_bin_cmd;

    let start = Instant::now();
    eprintln!("{{\"test\":\"test_cli_verify_command\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(1, true, false);

    // Run cass pages --verify
    let mut cmd = cargo_bin_cmd!("cass");
    let assert = cmd
        .arg("pages")
        .arg("--verify")
        .arg(&artifacts.bundle.site_dir)
        .arg("--json")
        .assert();

    assert.success();

    eprintln!("{{\"test\":\"test_cli_verify_command\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}

// =============================================================================
// Test: Search in Decrypted Archive
// =============================================================================

/// Test that we can query the decrypted export database.
#[test]
fn test_search_in_decrypted_archive() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_search_in_decrypted_archive\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(5, true, false);

    // Decrypt
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor =
        DecryptionEngine::unlock_with_password(config, TEST_PASSWORD).expect("unlock");
    let decrypted_path = temp_dir.path().join("decrypted.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt");

    // Open the export database directly (it has a different schema than the main DB)
    let conn = Connection::open(&decrypted_path).expect("open decrypted db");

    // Verify conversations table exists and has data
    let conv_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))
        .expect("count conversations");
    assert_eq!(conv_count, 5, "Should have 5 conversations");

    // Verify messages table exists and has data
    let msg_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .expect("count messages");
    assert!(msg_count > 0, "Should have messages");

    // Verify export_meta table has schema version
    let schema_version: String = conn
        .query_row(
            "SELECT value FROM export_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("get schema version");
    assert_eq!(schema_version, "1", "Export schema version should be 1");

    eprintln!("{{\"test\":\"test_search_in_decrypted_archive\",\"duration_ms\":{},\"status\":\"PASS\"}}", start.elapsed().as_millis());
}
