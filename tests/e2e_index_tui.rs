use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::Path;

mod util;
use util::EnvGuard;

fn make_codex_fixture(root: &Path) {
    let sessions = root.join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");
    let sample = r#"{"role":"user","timestamp":1700000000000,"content":"hello"}
{"role":"assistant","timestamp":1700000001000,"content":"hi"}
"#;
    fs::write(file, sample).unwrap();
}

#[test]
fn index_then_tui_once_headless() {
    let tmp = tempfile::TempDir::new().unwrap();
    // Isolate from the developer machine's real session dirs (HOME-based connectors).
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());

    let xdg = tmp.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let _guard_xdg = EnvGuard::set("XDG_DATA_HOME", xdg.to_string_lossy());

    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // Codex fixture under CODEX_HOME to satisfy detection.
    let _guard_codex = EnvGuard::set("CODEX_HOME", data_dir.to_string_lossy());
    make_codex_fixture(&data_dir);

    // Run index --full against this data dir.
    cargo_bin_cmd!("cass")
        .arg("index")
        .arg("--full")
        .arg("--data-dir")
        .arg(&data_dir)
        .assert()
        .success();

    // Smoke run TUI once in headless mode using the same data dir.
    cargo_bin_cmd!("cass")
        .arg("tui")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--once")
        .env("TUI_HEADLESS", "1")
        .assert()
        .success();

    // Ensure index artifacts exist.
    assert!(data_dir.join("agent_search.db").exists());
    assert!(data_dir.join("index/v6").exists());
}
