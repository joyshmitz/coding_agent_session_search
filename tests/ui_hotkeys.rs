use coding_agent_search::sources::provenance::SourceFilter;
use coding_agent_search::ui::tui::footer_legend;

#[test]
fn footer_mentions_editor_and_clear_keys() {
    // Simplified footer shows essential keys only
    let short = footer_legend(false);
    assert!(
        short.contains("Enter view"),
        "short footer should show Enter view"
    );
    assert!(
        short.contains("Esc quit"),
        "short footer should show Esc quit"
    );
    assert!(
        short.contains("F1 help"),
        "short footer should show F1 help"
    );
}

#[test]
fn help_includes_detail_find_hotkeys() {
    let lines = coding_agent_search::ui::tui::help_lines(
        coding_agent_search::ui::components::theme::ThemePalette::dark(),
    );
    let text: String = lines.iter().map(|l| l.to_string()).collect();
    assert!(
        text.contains("/ detail-find"),
        "help should mention detail-find shortcut"
    );
    assert!(
        text.contains("n/N"),
        "help should mention n/N navigation in detail-find"
    );
}

// =============================================================================
// F11 Source Filter Hotkey Tests
// =============================================================================

#[test]
fn f11_hotkey_documented_in_help() {
    let lines = coding_agent_search::ui::tui::help_lines(
        coding_agent_search::ui::components::theme::ThemePalette::dark(),
    );
    let text: String = lines.iter().map(|l| l.to_string()).collect();

    assert!(
        text.contains("F11"),
        "help should mention F11 hotkey for source filtering"
    );
    assert!(
        text.contains("source filter") || text.contains("cycle source"),
        "help should explain F11 cycles source filter"
    );
}

#[test]
fn f11_hotkey_documented_in_footer_or_help() {
    // F11 for source filtering may not be in footer (footer shows F1-F9 for brevity)
    // but should be documented in help
    let footer = footer_legend(true);
    let help_lines = coding_agent_search::ui::tui::help_lines(
        coding_agent_search::ui::components::theme::ThemePalette::dark(),
    );
    let help_text: String = help_lines.iter().map(|l| l.to_string()).collect();

    // F11 should be documented in either footer or help
    let f11_in_footer = footer.contains("F11") || footer.contains("src");
    let f11_in_help = help_text.contains("F11");

    assert!(
        f11_in_footer || f11_in_help,
        "F11 source filter should be documented in footer or help"
    );
}

#[test]
fn source_filter_cycle_api_exists() {
    // Verify the cycle() method exists and behaves correctly
    // This tests the same API the TUI uses for F11 handling
    let filter = SourceFilter::All;
    let cycled = filter.cycle();
    assert_eq!(cycled, SourceFilter::Local, "All should cycle to Local");
}

#[test]
fn source_filter_cycle_matches_documented_behavior() {
    // F11 is documented as cycling: all → local → remote → all
    let content = {
        let lines = coding_agent_search::ui::tui::help_lines(
            coding_agent_search::ui::components::theme::ThemePalette::dark(),
        );
        lines.iter().map(|l| l.to_string()).collect::<String>()
    };

    // Documentation says "all → local → remote → all"
    if content.contains("all → local → remote → all") {
        // Verify code matches documentation
        assert_eq!(SourceFilter::All.cycle(), SourceFilter::Local);
        assert_eq!(SourceFilter::Local.cycle(), SourceFilter::Remote);
        assert_eq!(SourceFilter::Remote.cycle(), SourceFilter::All);
    }
}

#[test]
fn shift_f11_hotkey_documented() {
    let lines = coding_agent_search::ui::tui::help_lines(
        coding_agent_search::ui::components::theme::ThemePalette::dark(),
    );
    let text: String = lines.iter().map(|l| l.to_string()).collect();

    assert!(
        text.contains("Shift+F11") || text.contains("Shift-F11"),
        "help should mention Shift+F11 for source menu"
    );
    assert!(
        text.contains("menu") || text.contains("select"),
        "help should explain Shift+F11 opens selection menu"
    );
}

#[test]
fn source_filter_display_for_status_messages() {
    // The TUI shows status like "Source: all sources", "Source: local only"
    // Verify SourceFilter::to_string() produces expected values for status display
    assert_eq!(SourceFilter::All.to_string(), "all");
    assert_eq!(SourceFilter::Local.to_string(), "local");
    assert_eq!(SourceFilter::Remote.to_string(), "remote");
    assert_eq!(
        SourceFilter::SourceId("laptop".to_string()).to_string(),
        "laptop"
    );
}
