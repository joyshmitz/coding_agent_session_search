//! Integration tests for Pages bundle building and preview server.
//!
//! These tests use real session fixtures to build bundles and exercise
//! the preview server lifecycle with ephemeral port binding.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test --test pages_preview_integration
//! ```

use anyhow::Result;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

mod util;
use util::e2e_log::{E2ePerformanceMetrics, PhaseTracker};

// ============================================
// Test Helpers
// ============================================

fn tracker_for(test_name: &str) -> PhaseTracker {
    PhaseTracker::new("pages_preview_integration", test_name)
}

/// Get an available ephemeral port by binding to port 0
fn get_ephemeral_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    let port = listener.local_addr().expect("get local addr").port();
    drop(listener);
    port
}

// NOTE: create_test_db helper removed - not currently used
// If needed for future tests, can be restored from git history

// ============================================
// Bundle Building Tests
// ============================================

/// Test that bundle builder creates correct directory structure
#[test]
fn bundle_creates_complete_structure() -> Result<()> {
    use coding_agent_search::pages::bundle::BundleBuilder;
    use coding_agent_search::pages::encrypt::EncryptionEngine;
    use std::fs;

    let tracker = tracker_for("bundle_creates_complete_structure");
    let _trace_guard = tracker.trace_env_guard();

    let phase_start = tracker.start("setup", Some("Create test fixtures"));
    let temp = tempfile::TempDir::new()?;
    let encrypted_dir = temp.path().join("encrypted");
    let bundle_dir = temp.path().join("bundle");

    fs::create_dir_all(&encrypted_dir)?;

    // Create a simple test file to encrypt
    let test_content = b"Test database content for bundle integration test";
    let test_file = temp.path().join("test_input.db");
    fs::write(&test_file, test_content)?;

    // Encrypt it
    let mut engine = EncryptionEngine::default();
    engine.add_password_slot("test-password-123")?;
    engine.encrypt_file(&test_file, &encrypted_dir, |_, _| {})?;

    fs::remove_file(&test_file)?;
    tracker.end("setup", Some("Create test fixtures"), phase_start);

    // Phase: Build bundle
    let phase_start = tracker.start("build_bundle", Some("Build the site bundle"));
    let build_start = std::time::Instant::now();

    let builder = BundleBuilder::new()
        .title("Integration Test Archive")
        .description("Test bundle from integration test");

    let result = builder.build(&encrypted_dir, &bundle_dir, |phase, msg| {
        eprintln!("  Bundle phase: {} - {}", phase, msg);
    })?;

    let build_duration = build_start.elapsed().as_millis() as u64;
    tracker.end("build_bundle", Some("Build the site bundle"), phase_start);

    // Phase: Verify structure
    let phase_start = tracker.start("verify", Some("Verify bundle structure"));

    // Verify directory structure
    assert!(
        result.site_dir.exists(),
        "site/ directory should exist at {}",
        result.site_dir.display()
    );
    assert!(
        result.private_dir.exists(),
        "private/ directory should exist at {}",
        result.private_dir.display()
    );

    // Verify required site files
    let required_files = [
        "index.html",
        "styles.css",
        "auth.js",
        "viewer.js",
        "search.js",
        "sw.js",
        "config.json",
        "site.json",
        "integrity.json",
        "robots.txt",
    ];

    for file in required_files {
        let path = result.site_dir.join(file);
        assert!(
            path.exists(),
            "Required file {} should exist in site/ at {}",
            file,
            path.display()
        );
    }

    // Verify payload directory
    let payload_dir = result.site_dir.join("payload");
    assert!(
        payload_dir.exists(),
        "payload/ directory should exist in site/"
    );
    assert!(
        result.chunk_count > 0,
        "Should have at least one payload chunk"
    );

    // Verify fingerprint format (16 hex chars)
    assert_eq!(
        result.fingerprint.len(),
        16,
        "Fingerprint should be 16 characters"
    );
    assert!(
        result.fingerprint.chars().all(|c| c.is_ascii_hexdigit()),
        "Fingerprint should be hexadecimal: {}",
        result.fingerprint
    );

    tracker.end("verify", Some("Verify bundle structure"), phase_start);

    tracker.metrics(
        "bundle_build",
        &E2ePerformanceMetrics::new()
            .with_duration(build_duration)
            .with_custom("chunk_count", serde_json::json!(result.chunk_count))
            .with_custom(
                "fingerprint_len",
                serde_json::json!(result.fingerprint.len()),
            ),
    );

    tracker.complete();
    Ok(())
}

/// Test bundle integrity manifest is valid
#[test]
fn bundle_integrity_manifest_valid() -> Result<()> {
    use coding_agent_search::pages::bundle::{BundleBuilder, IntegrityManifest};
    use coding_agent_search::pages::encrypt::EncryptionEngine;
    use std::fs;

    let tracker = tracker_for("bundle_integrity_manifest_valid");
    let _trace_guard = tracker.trace_env_guard();

    let phase_start = tracker.start("setup", Some("Create test fixtures"));
    let temp = tempfile::TempDir::new()?;
    let encrypted_dir = temp.path().join("encrypted");
    let bundle_dir = temp.path().join("bundle");

    fs::create_dir_all(&encrypted_dir)?;

    let test_file = temp.path().join("test.db");
    fs::write(&test_file, b"test database content")?;

    let mut engine = EncryptionEngine::default();
    engine.add_password_slot("password123")?;
    engine.encrypt_file(&test_file, &encrypted_dir, |_, _| {})?;
    fs::remove_file(&test_file)?;
    tracker.end("setup", Some("Create test fixtures"), phase_start);

    let phase_start = tracker.start("build", Some("Build bundle"));
    let builder = BundleBuilder::new();
    let result = builder.build(&encrypted_dir, &bundle_dir, |_, _| {})?;
    tracker.end("build", Some("Build bundle"), phase_start);

    let phase_start = tracker.start("verify_integrity", Some("Verify integrity manifest"));

    // Load integrity manifest
    let integrity_path = result.site_dir.join("integrity.json");
    assert!(integrity_path.exists(), "integrity.json should exist");

    let integrity_content = fs::read_to_string(&integrity_path)?;
    let manifest: IntegrityManifest = serde_json::from_str(&integrity_content)
        .expect("integrity.json should be valid JSON matching IntegrityManifest schema");

    // Verify schema version
    assert_eq!(
        manifest.version, 1,
        "Integrity manifest version should be 1"
    );

    // Verify it contains entries
    assert!(
        !manifest.files.is_empty(),
        "Integrity manifest should have file entries"
    );

    // Verify each entry has valid SHA256 hash (64 hex chars)
    for (path, entry) in &manifest.files {
        assert_eq!(
            entry.sha256.len(),
            64,
            "SHA256 for {} should be 64 hex chars, got {}",
            path,
            entry.sha256.len()
        );
        assert!(
            entry.sha256.chars().all(|c| c.is_ascii_hexdigit()),
            "SHA256 for {} should be hexadecimal",
            path
        );

        // Verify file exists and size matches
        let file_path = result.site_dir.join(path);
        assert!(
            file_path.exists(),
            "File {} listed in integrity.json should exist",
            path
        );

        let metadata = fs::metadata(&file_path)?;
        assert_eq!(
            metadata.len(),
            entry.size,
            "Size mismatch for {}: expected {}, got {}",
            path,
            entry.size,
            metadata.len()
        );
    }

    // Verify integrity.json itself is not in the manifest (chicken/egg)
    assert!(
        !manifest.files.contains_key("integrity.json"),
        "integrity.json should not be listed in its own manifest"
    );

    tracker.end(
        "verify_integrity",
        Some("Verify integrity manifest"),
        phase_start,
    );

    tracker.complete();
    Ok(())
}

/// Test bundle site.json metadata
#[test]
fn bundle_site_metadata_correct() -> Result<()> {
    use coding_agent_search::pages::bundle::BundleBuilder;
    use coding_agent_search::pages::encrypt::EncryptionEngine;
    use std::fs;

    let tracker = tracker_for("bundle_site_metadata_correct");
    let _trace_guard = tracker.trace_env_guard();

    let temp = tempfile::TempDir::new()?;
    let encrypted_dir = temp.path().join("encrypted");
    let bundle_dir = temp.path().join("bundle");

    fs::create_dir_all(&encrypted_dir)?;

    let test_file = temp.path().join("test.db");
    fs::write(&test_file, b"test content")?;

    let mut engine = EncryptionEngine::default();
    engine.add_password_slot("password")?;
    engine.encrypt_file(&test_file, &encrypted_dir, |_, _| {})?;
    fs::remove_file(&test_file)?;

    let builder = BundleBuilder::new()
        .title("Custom Title Here")
        .description("Custom description for testing");

    let result = builder.build(&encrypted_dir, &bundle_dir, |_, _| {})?;

    // Load and verify site.json
    let site_json_path = result.site_dir.join("site.json");
    let site_json_content = fs::read_to_string(&site_json_path)?;
    let site_json: serde_json::Value = serde_json::from_str(&site_json_content)?;

    assert_eq!(
        site_json["title"], "Custom Title Here",
        "title should match builder config"
    );
    assert_eq!(
        site_json["description"], "Custom description for testing",
        "description should match builder config"
    );
    assert_eq!(site_json["generator"], "cass", "generator should be 'cass'");

    tracker.complete();
    Ok(())
}

// ============================================
// Preview Server Tests
// ============================================

/// Test ephemeral port binding works correctly
#[test]
fn preview_ephemeral_port_binding() {
    let tracker = tracker_for("preview_ephemeral_port_binding");
    let _trace_guard = tracker.trace_env_guard();

    let phase_start = tracker.start("get_ports", Some("Get ephemeral ports"));

    // Get multiple ephemeral ports to verify uniqueness
    let port1 = get_ephemeral_port();
    let port2 = get_ephemeral_port();
    let port3 = get_ephemeral_port();

    assert!(port1 > 0, "Port 1 should be non-zero");
    assert!(port2 > 0, "Port 2 should be non-zero");
    assert!(port3 > 0, "Port 3 should be non-zero");

    // Ports should be different (with high probability)
    assert!(
        port1 != port2 || port2 != port3,
        "Ephemeral ports should generally be unique"
    );

    // Ports should be in ephemeral range (typically > 1024)
    assert!(port1 > 1024, "Ephemeral port {} should be > 1024", port1);

    tracker.end("get_ports", Some("Get ephemeral ports"), phase_start);

    tracker.metrics(
        "ephemeral_ports",
        &E2ePerformanceMetrics::new()
            .with_custom("port1", serde_json::json!(port1))
            .with_custom("port2", serde_json::json!(port2))
            .with_custom("port3", serde_json::json!(port3)),
    );

    tracker.complete();
}

/// Test preview config defaults
#[test]
fn preview_config_defaults() {
    use coding_agent_search::pages::preview::PreviewConfig;

    let tracker = tracker_for("preview_config_defaults");
    let _trace_guard = tracker.trace_env_guard();

    let config = PreviewConfig::default();

    assert_eq!(config.port, 8080, "Default port should be 8080");
    assert!(config.open_browser, "Default should open browser");
    assert_eq!(
        config.site_dir.to_string_lossy(),
        ".",
        "Default site_dir should be current directory"
    );

    tracker.complete();
}

/// Test preview error types display correctly
#[test]
fn preview_error_display() {
    use coding_agent_search::pages::preview::PreviewError;
    use std::io;
    use std::path::PathBuf;

    let tracker = tracker_for("preview_error_display");
    let _trace_guard = tracker.trace_env_guard();

    // Test BindFailed display
    let bind_err = PreviewError::BindFailed {
        port: 8080,
        source: io::Error::new(io::ErrorKind::AddrInUse, "port in use"),
    };
    let display = format!("{}", bind_err);
    assert!(
        display.contains("8080"),
        "BindFailed should mention port: {}",
        display
    );
    assert!(
        display.contains("port in use"),
        "BindFailed should include source: {}",
        display
    );

    // Test SiteDirectoryNotFound display
    let not_found_err = PreviewError::SiteDirectoryNotFound(PathBuf::from("/nonexistent/path"));
    let display = format!("{}", not_found_err);
    assert!(
        display.contains("/nonexistent/path"),
        "SiteDirectoryNotFound should show path: {}",
        display
    );

    // Test BrowserOpenFailed display
    let browser_err = PreviewError::BrowserOpenFailed("no browser found".to_string());
    let display = format!("{}", browser_err);
    assert!(
        display.contains("no browser found"),
        "BrowserOpenFailed should show message: {}",
        display
    );

    tracker.complete();
}

/// Test preview server can serve static files (requires async runtime)
#[tokio::test]
async fn preview_serves_static_files() -> Result<()> {
    use coding_agent_search::pages::preview::PreviewConfig;
    use std::fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let tracker = tracker_for("preview_serves_static_files");
    let _trace_guard = tracker.trace_env_guard();

    // Setup: Create a temp site directory with test files
    let phase_start = tracker.start("setup", Some("Create test site"));
    let temp = tempfile::TempDir::new()?;
    let site_dir = temp.path();

    fs::write(
        site_dir.join("index.html"),
        "<!doctype html><html><body>Test</body></html>",
    )?;
    fs::write(site_dir.join("styles.css"), "body { color: red; }")?;
    fs::write(site_dir.join("app.js"), "console.log('test');")?;
    fs::create_dir(site_dir.join("payload"))?;
    fs::write(site_dir.join("payload/chunk.bin"), [0u8; 1024])?;

    tracker.end("setup", Some("Create test site"), phase_start);

    // Start preview server on ephemeral port
    let phase_start = tracker.start("start_server", Some("Start preview server"));
    let port = get_ephemeral_port();

    let config = PreviewConfig {
        site_dir: site_dir.to_path_buf(),
        port,
        open_browser: false, // Don't try to open browser in test
    };

    // Start server in background task
    let server_running = Arc::new(AtomicBool::new(true));
    let server_running_clone = server_running.clone();

    let server_handle = tokio::spawn(async move {
        let _ = coding_agent_search::pages::preview::start_preview_server(config).await;
        server_running_clone.store(false, Ordering::SeqCst);
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    tracker.end("start_server", Some("Start preview server"), phase_start);

    // Test: Fetch index.html
    let phase_start = tracker.start("fetch_index", Some("Fetch index.html"));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;

    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    assert!(
        response_str.contains("HTTP/1.1 200 OK"),
        "Should get 200 OK for index.html: {}",
        &response_str[..200.min(n)]
    );
    assert!(
        response_str.contains("text/html"),
        "Content-Type should be text/html"
    );
    assert!(
        response_str.contains("Cross-Origin-Opener-Policy: same-origin"),
        "Should have COOP header"
    );
    assert!(
        response_str.contains("Cross-Origin-Embedder-Policy: require-corp"),
        "Should have COEP header"
    );

    tracker.end("fetch_index", Some("Fetch index.html"), phase_start);

    // Test: Fetch CSS file
    let phase_start = tracker.start("fetch_css", Some("Fetch CSS file"));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
    stream
        .write_all(b"GET /styles.css HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;

    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    assert!(
        response_str.contains("HTTP/1.1 200 OK"),
        "Should get 200 OK for styles.css"
    );
    assert!(
        response_str.contains("text/css"),
        "Content-Type should be text/css"
    );

    tracker.end("fetch_css", Some("Fetch CSS file"), phase_start);

    // Test: 404 for non-existent file
    let phase_start = tracker.start("fetch_404", Some("Verify 404 handling"));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
    stream
        .write_all(b"GET /nonexistent.txt HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;

    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    assert!(
        response_str.contains("404"),
        "Should get 404 for non-existent file"
    );

    tracker.end("fetch_404", Some("Verify 404 handling"), phase_start);

    // Cleanup: Abort server
    server_handle.abort();

    tracker.complete();
    Ok(())
}

/// Test preview server blocks directory traversal
#[tokio::test]
async fn preview_blocks_traversal() -> Result<()> {
    use coding_agent_search::pages::preview::PreviewConfig;
    use std::fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let tracker = tracker_for("preview_blocks_traversal");
    let _trace_guard = tracker.trace_env_guard();

    let temp = tempfile::TempDir::new()?;
    let site_dir = temp.path();

    // Create a file outside site_dir that we'll try to access
    let _outside_file = temp.path().join("../sensitive.txt");
    // Can't actually create a file outside tempdir, so just test the traversal is blocked

    fs::write(site_dir.join("index.html"), "<html></html>")?;

    let port = get_ephemeral_port();
    let config = PreviewConfig {
        site_dir: site_dir.to_path_buf(),
        port,
        open_browser: false,
    };

    let server_handle = tokio::spawn(async move {
        let _ = coding_agent_search::pages::preview::start_preview_server(config).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Try directory traversal
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
    stream
        .write_all(b"GET /../etc/passwd HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;

    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    assert!(
        response_str.contains("400") || response_str.contains("Invalid"),
        "Directory traversal should be blocked: {}",
        &response_str[..200.min(n)]
    );

    server_handle.abort();

    tracker.complete();
    Ok(())
}

// ============================================
// Integration Tests (Bundle + Preview)
// ============================================

/// Full integration: build bundle then serve via preview
#[tokio::test]
async fn integration_build_and_preview() -> Result<()> {
    use coding_agent_search::pages::bundle::BundleBuilder;
    use coding_agent_search::pages::encrypt::EncryptionEngine;
    use coding_agent_search::pages::preview::PreviewConfig;
    use std::fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let tracker = tracker_for("integration_build_and_preview");
    let _trace_guard = tracker.trace_env_guard();

    // Phase 1: Build a real bundle
    let phase_start = tracker.start("build_bundle", Some("Build encrypted bundle"));
    let temp = tempfile::TempDir::new()?;
    let encrypted_dir = temp.path().join("encrypted");
    let bundle_dir = temp.path().join("bundle");

    fs::create_dir_all(&encrypted_dir)?;

    let test_file = temp.path().join("test.db");
    fs::write(&test_file, b"Integration test database content")?;

    let mut engine = EncryptionEngine::default();
    engine.add_password_slot("integration-test-password")?;
    engine.encrypt_file(&test_file, &encrypted_dir, |_, _| {})?;
    fs::remove_file(&test_file)?;

    let builder = BundleBuilder::new()
        .title("Integration Test Bundle")
        .description("Built and served via integration test");

    let result = builder.build(&encrypted_dir, &bundle_dir, |_, _| {})?;
    tracker.end("build_bundle", Some("Build encrypted bundle"), phase_start);

    // Phase 2: Start preview server
    let phase_start = tracker.start("start_preview", Some("Start preview server"));
    let port = get_ephemeral_port();
    let config = PreviewConfig {
        site_dir: result.site_dir.clone(),
        port,
        open_browser: false,
    };

    let server_handle = tokio::spawn(async move {
        let _ = coding_agent_search::pages::preview::start_preview_server(config).await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    tracker.end("start_preview", Some("Start preview server"), phase_start);

    // Phase 3: Verify all bundle files are served correctly
    let phase_start = tracker.start("verify_served", Some("Verify files served correctly"));

    let files_to_check = [
        ("/", "text/html"),
        ("/index.html", "text/html"),
        ("/styles.css", "text/css"),
        ("/auth.js", "application/javascript"),
        ("/config.json", "application/json"),
        ("/site.json", "application/json"),
        ("/integrity.json", "application/json"),
    ];

    for (path, expected_type) in files_to_check {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
        let request = format!("GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n", path);
        stream.write_all(request.as_bytes()).await?;

        let mut response = vec![0u8; 8192];
        let n = stream.read(&mut response).await?;
        let response_str = String::from_utf8_lossy(&response[..n]);

        assert!(
            response_str.contains("200 OK"),
            "File {} should return 200 OK, got: {}",
            path,
            &response_str[..100.min(n)]
        );
        assert!(
            response_str.contains(expected_type),
            "File {} should have Content-Type {}, got: {}",
            path,
            expected_type,
            &response_str[..200.min(n)]
        );
    }

    tracker.end(
        "verify_served",
        Some("Verify files served correctly"),
        phase_start,
    );

    server_handle.abort();

    tracker.metrics(
        "integration",
        &E2ePerformanceMetrics::new()
            .with_custom("port", serde_json::json!(port))
            .with_custom("chunk_count", serde_json::json!(result.chunk_count)),
    );

    tracker.complete();
    Ok(())
}
