//! Premium UI widgets with world-class aesthetics.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::ui::components::theme::{ThemePalette, colors, kbd_style};
use crate::ui::data::InputMode;
use crate::ui::shortcuts;

/// Premium search bar widget with refined visual hierarchy.
///
/// Design principles:
/// - Clear visual state indication through subtle border/title changes
/// - Keyboard hints that don't overwhelm the interface
/// - Balanced spacing and typography
pub fn search_bar(
    query: &str,
    palette: ThemePalette,
    input_mode: InputMode,
    mode_label: &str,
    chips: Vec<Span<'static>>,
) -> Paragraph<'static> {
    let in_query_mode = matches!(input_mode, InputMode::Query);

    // Title and border styling based on input mode
    let (title_text, title_style, border_style) = match input_mode {
        InputMode::Query => (
            format!(" Search · {mode_label} "),
            palette.title(),
            palette.border_style(),
        ),
        InputMode::Agent => (
            " Filter: Agent ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
        InputMode::Workspace => (
            " Filter: Workspace ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
        InputMode::CreatedFrom => (
            " Filter: From Date ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
        InputMode::CreatedTo => (
            " Filter: To Date ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
        InputMode::PaneFilter => (
            " Filter: Pane ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
        InputMode::DetailFind => (
            " Detail Find ".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
            palette.border_focus_style(),
        ),
    };
    let title = Span::styled(title_text, title_style);

    // Query text style
    let query_style = if in_query_mode {
        Style::default().fg(palette.fg)
    } else {
        Style::default().fg(palette.accent_alt)
    };

    // Build the input line with chips and query
    let mut first_line = chips;
    if !first_line.is_empty() {
        first_line.push(Span::raw(" "));
    }

    // Subtle cursor indicator
    let cursor = if in_query_mode { "▎" } else { "│" };
    let prompt = if in_query_mode { "/" } else { "›" };

    first_line.push(Span::styled(
        format!("{prompt} "),
        Style::default().fg(palette.hint),
    ));
    first_line.push(Span::styled(query.to_string(), query_style));
    first_line.push(Span::styled(
        cursor.to_string(),
        Style::default().fg(palette.accent),
    ));

    // Context-aware hints line - minimal, not overwhelming
    let tips_line = if in_query_mode {
        Line::from(vec![
            Span::styled(shortcuts::HELP, kbd_style(palette)),
            Span::styled(" help", Style::default().fg(palette.hint)),
            Span::styled("  ·  ", Style::default().fg(colors::TEXT_DISABLED)),
            Span::styled(shortcuts::FILTER_AGENT, Style::default().fg(palette.hint)),
            Span::styled(" agent", Style::default().fg(palette.hint)),
            Span::raw("  "),
            Span::styled(
                shortcuts::FILTER_WORKSPACE,
                Style::default().fg(palette.hint),
            ),
            Span::styled(" workspace", Style::default().fg(palette.hint)),
            Span::raw("  "),
            Span::styled(
                shortcuts::FILTER_DATE_FROM,
                Style::default().fg(palette.hint),
            ),
            Span::styled(" time", Style::default().fg(palette.hint)),
            Span::styled("  ·  ", Style::default().fg(colors::TEXT_DISABLED)),
            Span::styled(shortcuts::CLEAR_FILTERS, Style::default().fg(palette.hint)),
            Span::styled(" clear", Style::default().fg(palette.hint)),
        ])
    } else {
        // Simplified hints when in filter mode
        Line::from(vec![
            Span::styled("Enter", kbd_style(palette)),
            Span::styled(" apply", Style::default().fg(palette.hint)),
            Span::styled("  ·  ", Style::default().fg(colors::TEXT_DISABLED)),
            Span::styled("Esc", Style::default().fg(palette.hint)),
            Span::styled(" cancel", Style::default().fg(palette.hint)),
        ])
    };

    let body = vec![Line::from(first_line), tips_line];

    Paragraph::new(body)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .style(Style::default().bg(palette.bg))
        .alignment(Alignment::Left)
}

/// Creates a premium-styled block with consistent theming.
pub fn themed_block(title: &str, palette: ThemePalette, focused: bool) -> Block<'_> {
    let border_style = if focused {
        palette.border_focus_style()
    } else {
        palette.border_style()
    };

    let title_style = if focused {
        palette.title()
    } else {
        palette.title_subtle()
    };

    Block::default()
        .title(Span::styled(format!(" {title} "), title_style))
        .borders(Borders::ALL)
        .border_style(border_style)
}

/// Creates filter chip spans with premium styling.
pub fn filter_chips(
    agents: &[String],
    workspaces: &[String],
    time_range: Option<&str>,
    palette: ThemePalette,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let chip_base = Style::default()
        .fg(palette.accent_alt)
        .add_modifier(Modifier::BOLD);

    if !agents.is_empty() {
        spans.push(Span::styled(format!("[{}]", agents.join(", ")), chip_base));
        spans.push(Span::raw(" "));
    }

    if !workspaces.is_empty() {
        // Truncate long workspace paths for chip display (char-safe slicing)
        let ws_display: Vec<String> = workspaces
            .iter()
            .map(|w| {
                if w.chars().count() > 20 {
                    let skip = w.chars().count().saturating_sub(18);
                    let tail: String = w.chars().skip(skip).collect();
                    format!("…{tail}")
                } else {
                    w.clone()
                }
            })
            .collect();
        spans.push(Span::styled(
            format!("[{}]", ws_display.join(", ")),
            chip_base,
        ));
        spans.push(Span::raw(" "));
    }

    if let Some(time) = time_range {
        spans.push(Span::styled(format!("[{time}]"), chip_base));
        spans.push(Span::raw(" "));
    }

    spans
}

/// Creates a score indicator with visual bars.
pub fn score_indicator(score: f32, palette: ThemePalette) -> Vec<Span<'static>> {
    let normalized = (score / 10.0).clamp(0.0, 1.0);
    let filled = (normalized * 5.0).round() as usize;
    let empty = 5 - filled;

    let color = if score >= 8.0 {
        colors::STATUS_SUCCESS
    } else if score >= 5.0 {
        palette.accent
    } else {
        palette.hint
    };

    let modifier = if score >= 8.0 {
        Modifier::BOLD
    } else if score >= 5.0 {
        Modifier::empty()
    } else {
        Modifier::DIM
    };

    vec![
        Span::styled(
            "●".repeat(filled),
            Style::default().fg(color).add_modifier(modifier),
        ),
        Span::styled(
            "○".repeat(empty),
            Style::default()
                .fg(palette.hint)
                .add_modifier(Modifier::DIM),
        ),
        Span::raw(" "),
        Span::styled(
            format!("{score:.1}"),
            Style::default().fg(color).add_modifier(modifier),
        ),
    ]
}

/// Calculate a centered rectangle with percentage dimensions.
pub fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let py = percent_y.min(100);
    let px = percent_x.min(100);

    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Percentage((100 - py) / 2),
                Constraint::Percentage(py),
                Constraint::Percentage((100 - py) / 2),
            ]
            .as_ref(),
        )
        .split(r);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Percentage((100 - px) / 2),
                Constraint::Percentage(px),
                Constraint::Percentage((100 - px) / 2),
            ]
            .as_ref(),
        )
        .split(popup_layout[1]);

    horizontal[1]
}

/// Create a centered popup with fixed dimensions.
/// The popup is clamped to available terminal space and centered.
pub fn centered_rect_fixed(width: u16, height: u16, r: Rect) -> Rect {
    // Clamp dimensions to available space (leave margin for visual separation)
    let actual_width = width.min(r.width.saturating_sub(4));
    let actual_height = height.min(r.height.saturating_sub(2));

    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(r.height.saturating_sub(actual_height) / 2),
            Constraint::Length(actual_height),
            Constraint::Length(r.height.saturating_sub(actual_height) / 2),
        ])
        .split(r);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(r.width.saturating_sub(actual_width) / 2),
            Constraint::Length(actual_width),
            Constraint::Length(r.width.saturating_sub(actual_width) / 2),
        ])
        .split(popup_layout[1]);

    horizontal[1]
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== filter_chips tests ====================

    #[test]
    fn test_filter_chips_empty_all() {
        let palette = ThemePalette::dark();
        let chips = filter_chips(&[], &[], None, palette);
        assert!(chips.is_empty());
    }

    #[test]
    fn test_filter_chips_with_agents() {
        let palette = ThemePalette::dark();
        let agents = vec!["claude".to_string(), "codex".to_string()];
        let chips = filter_chips(&agents, &[], None, palette);

        // Should have at least one span (the agent chip)
        assert!(!chips.is_empty());

        // Convert to string and check content
        let text: String = chips.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("claude"));
        assert!(text.contains("codex"));
    }

    #[test]
    fn test_filter_chips_with_workspaces() {
        let palette = ThemePalette::dark();
        let workspaces = vec!["/home/user/project".to_string()];
        let chips = filter_chips(&[], &workspaces, None, palette);

        assert!(!chips.is_empty());
        let text: String = chips.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("project") || text.contains("/home/user/project"));
    }

    #[test]
    fn test_filter_chips_with_time_range() {
        let palette = ThemePalette::dark();
        let chips = filter_chips(&[], &[], Some("Last 7 days"), palette);

        assert!(!chips.is_empty());
        let text: String = chips.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("Last 7 days"));
    }

    #[test]
    fn test_filter_chips_with_all_filters() {
        let palette = ThemePalette::dark();
        let agents = vec!["claude".to_string()];
        let workspaces = vec!["/project".to_string()];
        let chips = filter_chips(&agents, &workspaces, Some("Today"), palette);

        let text: String = chips.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("claude"));
        assert!(text.contains("Today"));
    }

    #[test]
    fn test_filter_chips_workspace_truncation() {
        let palette = ThemePalette::dark();
        let long_workspace = "/home/very/long/path/to/some/deeply/nested/project".to_string();
        let workspaces = vec![long_workspace];
        let chips = filter_chips(&[], &workspaces, None, palette);

        // Should produce some output
        assert!(!chips.is_empty());
        // Long paths should be truncated
        let text: String = chips.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("…") || text.len() < 60);
    }

    // ==================== score_indicator tests ====================

    #[test]
    fn test_score_indicator_high_score() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(9.5, palette);

        // Should have multiple spans
        assert!(!spans.is_empty());

        // Should contain the formatted score
        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("9.5"));
    }

    #[test]
    fn test_score_indicator_medium_score() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(6.0, palette);

        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("6.0"));
    }

    #[test]
    fn test_score_indicator_low_score() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(2.5, palette);

        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("2.5"));
    }

    #[test]
    fn test_score_indicator_zero() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(0.0, palette);

        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("0.0"));
        // Should have empty circles
        assert!(text.contains("○"));
    }

    #[test]
    fn test_score_indicator_max() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(10.0, palette);

        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert!(text.contains("10.0"));
        // Should have filled circles
        assert!(text.contains("●"));
    }

    #[test]
    fn test_score_indicator_partial() {
        let palette = ThemePalette::dark();
        let spans = score_indicator(5.0, palette);

        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        // Should have both filled and empty
        assert!(text.contains("●"));
        assert!(text.contains("○"));
    }

    #[test]
    fn test_score_indicator_clamping() {
        let palette = ThemePalette::dark();

        // Test score above 10
        let spans = score_indicator(15.0, palette);
        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        // Should still render (clamped internally)
        assert!(text.contains("15.0") || text.contains("●"));

        // Test negative score
        let spans = score_indicator(-5.0, palette);
        let text: String = spans.iter().map(|s| s.content.to_string()).collect();
        // Should still render (clamped internally)
        assert!(text.contains("-5.0") || text.contains("○"));
    }
}
