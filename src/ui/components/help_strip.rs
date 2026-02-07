//! Contextual help strip rendering.

use ftui::core::geometry::Rect;
use ftui::layout::{Constraint, Direction};
use ftui::text::{Line, Span};
use ftui::widgets::block::Block;
use ftui::widgets::borders::Borders;
use ftui::widgets::paragraph::Paragraph;
use ftui::widgets::Widget;
use ftui::{Frame, Style};

use crate::ui::components::theme::ThemePalette;

/// Render the help strip given a list of (key, label) pairs.
pub fn draw_help_strip(
    f: &mut Frame,
    area: Rect,
    shortcuts: &[(String, String)],
    palette: ThemePalette,
    pinned: bool,
) {
    let spans: Vec<Span> = shortcuts
        .iter()
        .flat_map(|(key, label)| {
            vec![
                Span::styled(
                    format!(" {key} "),
                    Style::new().fg(palette.fg).bg(palette.surface).bold(),
                ),
                Span::styled(format!("{label}  "), Style::new().fg(palette.hint)),
            ]
        })
        .collect();

    let block = Block::new()
        .borders(Borders::TOP)
        .title(if pinned { "Help (pinned)" } else { "Help" })
        .style(Style::new().fg(palette.hint));

    let para = Paragraph::new(Line::from_spans(spans)).block(block);
    para.render(area, f);
}

/// Compute layout to allocate a single-line help strip at bottom.
pub fn help_strip_area(area: Rect) -> Rect {
    let chunks = ftui::layout::Layout::new()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(area);
    chunks[1]
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== help_strip_area tests ====================

    #[test]
    fn test_help_strip_area_returns_bottom_row() {
        let area = Rect::new(0, 0, 80, 24);
        let strip = help_strip_area(area);

        // Strip should be at the bottom
        assert_eq!(strip.y, 23);
        // Strip should be full width
        assert_eq!(strip.width, 80);
        // Strip should be 1 row high
        assert_eq!(strip.height, 1);
    }

    #[test]
    fn test_help_strip_area_small_area() {
        let area = Rect::new(0, 0, 40, 10);
        let strip = help_strip_area(area);

        assert_eq!(strip.y, 9);
        assert_eq!(strip.width, 40);
        assert_eq!(strip.height, 1);
    }

    #[test]
    fn test_help_strip_area_with_offset() {
        let area = Rect::new(10, 5, 60, 15);
        let strip = help_strip_area(area);

        // X should be preserved
        assert_eq!(strip.x, 10);
        // Y should be at bottom of area (5 + 15 - 1 = 19)
        assert_eq!(strip.y, 19);
        assert_eq!(strip.width, 60);
        assert_eq!(strip.height, 1);
    }

    #[test]
    fn test_help_strip_area_minimum_height() {
        // Even with height of 2, should work
        let area = Rect::new(0, 0, 80, 2);
        let strip = help_strip_area(area);

        assert_eq!(strip.height, 1);
        assert_eq!(strip.y, 1);
    }

    #[test]
    fn test_help_strip_area_preserves_width() {
        for width in [40, 80, 120, 200] {
            let area = Rect::new(0, 0, width, 24);
            let strip = help_strip_area(area);
            assert_eq!(strip.width, width);
        }
    }
}
