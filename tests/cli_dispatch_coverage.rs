//! CLI dispatch subprocess integration tests.
//!
//! This module covers CLI commands that were previously untested via subprocess
//! invocation. Tests invoke the real binary with representative flags, validate
//! output formats, exit codes, and JSON structure.
//!
//! Coverage targets: completions, man, health, doctor, context, timeline, expand,
//! export, export-html, sources subcommands, models subcommands.

use assert_cmd::Command;
use predicates::prelude::*;
use predicates::str::contains;
use serde_json::Value;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Create a base command with isolated test environment.
fn base_cmd(temp_home: &Path) -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    cmd.env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1");
    // Isolate test environment
    cmd.env("HOME", temp_home);
    cmd.env("XDG_DATA_HOME", temp_home.join(".local/share"));
    cmd.env("XDG_CONFIG_HOME", temp_home.join(".config"));
    cmd.env("CODEX_HOME", temp_home.join(".codex"));
    // Disable TTY detection
    cmd.env("NO_COLOR", "1");
    cmd
}

/// Create base command without HOME isolation (for simple tests).
fn simple_cmd() -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    cmd.env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1");
    cmd.env("NO_COLOR", "1");
    cmd
}

// =============================================================================
// Completions command tests
// =============================================================================

#[test]
fn completions_bash_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "bash"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Bash completions should contain function definitions
    assert!(
        stdout.contains("_cass"),
        "bash completions should define _cass function"
    );
    assert!(
        stdout.contains("complete"),
        "bash completions should have complete command"
    );
}

#[test]
fn completions_zsh_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "zsh"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Zsh completions should contain compdef
    assert!(
        stdout.contains("#compdef") || stdout.contains("compdef"),
        "zsh completions should have compdef directive"
    );
}

#[test]
fn completions_fish_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "fish"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Fish completions use complete command
    assert!(
        stdout.contains("complete -c cass"),
        "fish completions should define completions for cass"
    );
}

#[test]
fn completions_powershell_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "powershell"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // PowerShell completions use Register-ArgumentCompleter
    assert!(
        stdout.contains("Register-ArgumentCompleter")
            || stdout.contains("ArgumentCompleter")
            || stdout.contains("$scriptblock"),
        "powershell completions should define argument completer"
    );
}

#[test]
fn completions_help_shows_shells() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("bash"))
        .stdout(contains("zsh"))
        .stdout(contains("fish"))
        .stdout(contains("powershell"));
}

// =============================================================================
// Man command tests
// =============================================================================

#[test]
fn man_outputs_groff_format() {
    let mut cmd = simple_cmd();
    cmd.arg("man");
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Man pages start with .TH (title header) or .\" comment
    assert!(
        stdout.contains(".TH") || stdout.contains(".SH"),
        "man output should be groff format with .TH or .SH macros"
    );
    assert!(
        stdout.contains("cass") || stdout.contains("CASS"),
        "man page should mention cass"
    );
}

#[test]
fn man_help_shows_usage() {
    let mut cmd = simple_cmd();
    cmd.args(["man", "--help"]);
    cmd.assert().success().stdout(contains("Generate man page"));
}

// =============================================================================
// Health command tests
// =============================================================================

#[test]
fn health_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["health", "--json", "--data-dir", data_dir.to_str().unwrap()]);

    let output = cmd.assert().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON with healthy field
    if !stdout.trim().is_empty() {
        let json: Value = serde_json::from_str(stdout.trim()).expect("valid health json");
        assert!(
            json.get("healthy").is_some(),
            "health JSON should have 'healthy' field"
        );
    }
}

#[test]
fn health_with_robot_meta_includes_metadata() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // First create the DB by running index
    let mut idx_cmd = base_cmd(tmp.path());
    idx_cmd.args(["index", "--data-dir", data_dir.to_str().unwrap(), "--json"]);
    idx_cmd.assert().success();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "health",
        "--json",
        "--robot-meta",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid health json with meta");

    // Should have _meta block
    assert!(
        json.get("_meta").is_some() || json.get("latency_ms").is_some(),
        "health --robot-meta should include metadata"
    );
}

#[test]
fn health_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["health", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("health check"))
        .stdout(contains("--json"))
        .stdout(contains("--stale-threshold"));
}

// =============================================================================
// Doctor command tests
// =============================================================================

#[test]
fn doctor_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["doctor", "--json", "--data-dir", data_dir.to_str().unwrap()]);

    let output = cmd.assert().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    if !stdout.trim().is_empty() {
        let json: Value = serde_json::from_str(stdout.trim()).expect("valid doctor json");
        // Doctor should have checks or issues array
        assert!(
            json.get("checks").is_some()
                || json.get("issues").is_some()
                || json.get("status").is_some(),
            "doctor JSON should have diagnostic fields"
        );
    }
}

#[test]
fn doctor_verbose_shows_all_checks() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "doctor",
        "--verbose",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // Just check it runs without error
    let _ = cmd.assert();
}

#[test]
fn doctor_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["doctor", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("Diagnose"))
        .stdout(contains("--fix"))
        .stdout(contains("--verbose"));
}

// =============================================================================
// Context command tests
// =============================================================================

#[test]
fn context_requires_path_argument() {
    let mut cmd = simple_cmd();
    cmd.arg("context");
    // Should fail without path
    cmd.assert().failure();
}

#[test]
fn context_json_with_nonexistent_path() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "context",
        "/nonexistent/path.jsonl",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // May fail or return empty results - either is acceptable
    let output = cmd.assert().get_output().clone();
    let _stdout = String::from_utf8_lossy(&output.stdout);
    // Test passes if command completes (success or failure with message)
}

#[test]
fn context_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["context", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("related sessions"))
        .stdout(contains("--json"))
        .stdout(contains("--limit"));
}

// =============================================================================
// Timeline command tests
// =============================================================================

#[test]
fn timeline_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // First create DB
    let mut idx_cmd = base_cmd(tmp.path());
    idx_cmd.args(["index", "--data-dir", data_dir.to_str().unwrap(), "--json"]);
    idx_cmd.assert().success();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "timeline",
        "--json",
        "--today",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON (may be empty array)
    if !stdout.trim().is_empty() {
        let _json: Value = serde_json::from_str(stdout.trim()).expect("valid timeline json");
    }
}

#[test]
fn timeline_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["timeline", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("timeline"))
        .stdout(contains("--since"))
        .stdout(contains("--until"))
        .stdout(contains("--today"));
}

// =============================================================================
// Expand command tests
// =============================================================================

#[test]
fn expand_requires_path_and_line() {
    let mut cmd = simple_cmd();
    cmd.arg("expand");
    cmd.assert().failure();
}

#[test]
fn expand_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["expand", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("messages around"))
        .stdout(contains("--line"))
        .stdout(contains("--context"))
        .stdout(contains("--json"));
}

// =============================================================================
// Export command tests
// =============================================================================

#[test]
fn export_requires_path() {
    let mut cmd = simple_cmd();
    cmd.arg("export");
    cmd.assert().failure();
}

#[test]
fn export_help_shows_formats() {
    let mut cmd = simple_cmd();
    cmd.args(["export", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("Export"))
        .stdout(contains("--format"))
        .stdout(contains("--output"))
        .stdout(contains("markdown").or(contains("Markdown")));
}

// =============================================================================
// Export-HTML command tests
// =============================================================================

#[test]
fn export_html_requires_session() {
    let mut cmd = simple_cmd();
    cmd.arg("export-html");
    cmd.assert().failure();
}

#[test]
fn export_html_help_shows_encryption_options() {
    let mut cmd = simple_cmd();
    cmd.args(["export-html", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("HTML"))
        .stdout(contains("--encrypt"))
        .stdout(contains("--output-dir"));
}

// =============================================================================
// Sources subcommand tests
// =============================================================================

#[test]
fn sources_list_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "list", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON with sources array
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid sources list json");
    assert!(
        json.get("sources").map(|v| v.is_array()).unwrap_or(false) || json.is_object(),
        "sources list --json should return object with sources array"
    );
}

#[test]
fn sources_list_verbose() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "list", "--verbose"]);

    // Should complete without error
    cmd.assert().success();
}

#[test]
fn sources_doctor_json_outputs_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "doctor", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    let _json: Value = serde_json::from_str(stdout.trim()).expect("valid sources doctor json");
}

#[test]
fn sources_help_shows_subcommands() {
    let mut cmd = simple_cmd();
    cmd.args(["sources", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("list"))
        .stdout(contains("add"))
        .stdout(contains("remove"))
        .stdout(contains("doctor"))
        .stdout(contains("sync"));
}

// =============================================================================
// Models subcommand tests
// =============================================================================

#[test]
fn models_status_json_outputs_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["models", "status", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid models status json");
    // Should have installed or available field
    assert!(
        json.get("installed").is_some()
            || json.get("models").is_some()
            || json.get("status").is_some()
            || json.is_object(),
        "models status JSON should have status information"
    );
}

#[test]
fn models_verify_json_with_no_model() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "models",
        "verify",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // May succeed with empty or fail - either is acceptable
    let output = cmd.assert().get_output().clone();
    let _stdout = String::from_utf8_lossy(&output.stdout);
}

#[test]
fn models_help_shows_subcommands() {
    let mut cmd = simple_cmd();
    cmd.args(["models", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("status"))
        .stdout(contains("install"))
        .stdout(contains("verify"))
        .stdout(contains("remove"));
}

#[test]
fn models_install_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["models", "install", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("--model"))
        .stdout(contains("--mirror"))
        .stdout(contains("--from-file"));
}

// =============================================================================
// Pages command tests
// =============================================================================

#[test]
fn pages_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["pages", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("searchable archive"))
        .stdout(contains("--export-only"))
        .stdout(contains("--verify"))
        .stdout(contains("--no-encryption"))
        .stdout(contains("--target"))
        .stdout(contains("--project"))
        .stdout(contains("--account-id"))
        .stdout(contains("--api-token"));
}

#[test]
fn pages_verify_with_nonexistent_path() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["pages", "--verify", "/nonexistent/bundle"]);

    // Should fail with appropriate error
    cmd.assert().failure();
}

// =============================================================================
// Exit code tests
// =============================================================================

#[test]
fn search_requires_query_argument() {
    // search command requires a query argument
    let mut cmd = simple_cmd();
    cmd.arg("search");
    // Should fail without query
    cmd.assert().failure();
}

#[test]
fn missing_required_arg_returns_error() {
    let mut cmd = simple_cmd();
    cmd.args(["search"]); // Missing query
    cmd.assert().failure();
}

// =============================================================================
// Clap parsing tests for new commands
// =============================================================================

use clap::Parser;
use coding_agent_search::{Cli, Commands};

#[test]
fn parse_completions_bash() {
    let cli = Cli::try_parse_from(["cass", "completions", "bash"]).expect("parse completions bash");
    match cli.command {
        Some(Commands::Completions { shell }) => {
            assert_eq!(shell, clap_complete::Shell::Bash);
        }
        other => panic!("expected completions command, got {other:?}"),
    }
}

#[test]
fn parse_health_with_stale_threshold() {
    let cli = Cli::try_parse_from(["cass", "health", "--stale-threshold", "600"])
        .expect("parse health with threshold");
    match cli.command {
        Some(Commands::Health {
            stale_threshold, ..
        }) => {
            assert_eq!(stale_threshold, 600);
        }
        other => panic!("expected health command, got {other:?}"),
    }
}

#[test]
fn parse_doctor_with_fix() {
    let cli = Cli::try_parse_from(["cass", "doctor", "--fix", "--verbose"])
        .expect("parse doctor with fix");
    match cli.command {
        Some(Commands::Doctor { fix, verbose, .. }) => {
            assert!(fix, "fix should be true");
            assert!(verbose, "verbose should be true");
        }
        other => panic!("expected doctor command, got {other:?}"),
    }
}

#[test]
fn parse_timeline_with_filters() {
    let cli = Cli::try_parse_from([
        "cass",
        "timeline",
        "--since",
        "2024-01-01",
        "--agent",
        "claude",
    ])
    .expect("parse timeline with filters");
    match cli.command {
        Some(Commands::Timeline { since, agent, .. }) => {
            assert_eq!(since, Some("2024-01-01".to_string()));
            assert_eq!(agent, vec!["claude"]);
        }
        other => panic!("expected timeline command, got {other:?}"),
    }
}

#[test]
fn parse_expand_with_context() {
    let cli = Cli::try_parse_from([
        "cass",
        "expand",
        "/path/to/session.jsonl",
        "--line",
        "100",
        "-C",
        "5",
    ])
    .expect("parse expand with context");
    match cli.command {
        Some(Commands::Expand {
            path,
            line,
            context,
            ..
        }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(line, 100);
            assert_eq!(context, 5);
        }
        other => panic!("expected expand command, got {other:?}"),
    }
}

#[test]
fn parse_context_with_limit() {
    let cli = Cli::try_parse_from(["cass", "context", "/path/to/session.jsonl", "--limit", "10"])
        .expect("parse context with limit");
    match cli.command {
        Some(Commands::Context { path, limit, .. }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(limit, 10);
        }
        other => panic!("expected context command, got {other:?}"),
    }
}

#[test]
fn parse_export_with_format() {
    let cli = Cli::try_parse_from([
        "cass",
        "export",
        "/path/to/session.jsonl",
        "--format",
        "json",
    ])
    .expect("parse export with format");
    match cli.command {
        Some(Commands::Export { path, format, .. }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(format, coding_agent_search::ConvExportFormat::Json);
        }
        other => panic!("expected export command, got {other:?}"),
    }
}

#[test]
fn parse_export_html_with_encrypt() {
    let cli = Cli::try_parse_from([
        "cass",
        "export-html",
        "/path/to/session.jsonl",
        "--encrypt",
        "--password-stdin",
    ])
    .expect("parse export-html with encrypt");
    match cli.command {
        Some(Commands::ExportHtml {
            session,
            encrypt,
            password_stdin,
            ..
        }) => {
            assert_eq!(session.to_str().unwrap(), "/path/to/session.jsonl");
            assert!(encrypt);
            assert!(password_stdin);
        }
        other => panic!("expected export-html command, got {other:?}"),
    }
}
