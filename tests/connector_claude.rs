use coding_agent_search::connectors::claude_code::ClaudeCodeConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::path::PathBuf;

#[test]
fn claude_parses_project_fixture() {
    let fixture_root = PathBuf::from("tests/fixtures/claude_project");
    let conn = ClaudeCodeConnector::new();
    let ctx = ScanContext {
        data_root: fixture_root.clone(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.title.as_deref(), Some("Claude Project A"));
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.messages[0].content, "Hi Claude");
}
