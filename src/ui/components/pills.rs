//! Filter pill rendering helpers.

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    widgets::{Block, BorderType, Borders, Paragraph},
};

use crate::ui::components::theme::ThemePalette;

#[derive(Clone, Debug)]
pub struct Pill {
    pub label: String,
    pub value: String,
    pub active: bool,
    pub editable: bool,
}

/// Render pills in a single row. Caller controls focus/interaction; returns rects for click hit-testing.
pub fn draw_pills(
    f: &mut Frame<'_>,
    area: Rect,
    pills: &[Pill],
    palette: ThemePalette,
) -> Vec<Rect> {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            pills
                .iter()
                .map(|_| Constraint::Length(20))
                .chain(std::iter::once(Constraint::Min(0)))
                .collect::<Vec<_>>(),
        )
        .split(area);

    let mut rects = Vec::new();
    for (idx, pill) in pills.iter().enumerate() {
        if idx >= chunks.len() {
            break;
        }
        let bg = if pill.active {
            palette.surface
        } else {
            palette.bg
        };
        let border_color = if pill.active {
            palette.accent
        } else {
            palette.border
        };
        let text_color = if pill.active {
            palette.fg
        } else {
            palette.hint
        };
        let content = format!("{}: {}", pill.label, pill.value);
        let para = Paragraph::new(content).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(if pill.active {
                    BorderType::Rounded
                } else {
                    BorderType::Plain
                })
                .border_style(Style::default().fg(border_color))
                .style(
                    Style::default()
                        .fg(text_color)
                        .bg(bg)
                        .add_modifier(if pill.editable {
                            Modifier::ITALIC
                        } else {
                            Modifier::empty()
                        }),
                ),
        );
        f.render_widget(para, chunks[idx]);
        rects.push(chunks[idx]);
    }
    rects
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Pill struct tests ====================

    #[test]
    fn test_pill_creation() {
        let pill = Pill {
            label: "Agent".to_string(),
            value: "claude".to_string(),
            active: true,
            editable: false,
        };

        assert_eq!(pill.label, "Agent");
        assert_eq!(pill.value, "claude");
        assert!(pill.active);
        assert!(!pill.editable);
    }

    #[test]
    fn test_pill_clone() {
        let pill = Pill {
            label: "Workspace".to_string(),
            value: "/home/user".to_string(),
            active: false,
            editable: true,
        };

        let cloned = pill.clone();
        assert_eq!(cloned.label, pill.label);
        assert_eq!(cloned.value, pill.value);
        assert_eq!(cloned.active, pill.active);
        assert_eq!(cloned.editable, pill.editable);
    }

    #[test]
    fn test_pill_debug() {
        let pill = Pill {
            label: "Test".to_string(),
            value: "Value".to_string(),
            active: true,
            editable: true,
        };

        let debug_str = format!("{:?}", pill);
        assert!(debug_str.contains("Pill"));
        assert!(debug_str.contains("Test"));
        assert!(debug_str.contains("Value"));
    }

    #[test]
    fn test_pill_with_empty_strings() {
        let pill = Pill {
            label: "".to_string(),
            value: "".to_string(),
            active: false,
            editable: false,
        };

        assert!(pill.label.is_empty());
        assert!(pill.value.is_empty());
    }

    #[test]
    fn test_pill_with_special_characters() {
        let pill = Pill {
            label: "Path".to_string(),
            value: "/home/user/my project/src".to_string(),
            active: true,
            editable: false,
        };

        assert!(pill.value.contains(' '));
        assert!(pill.value.contains('/'));
    }

    #[test]
    fn test_pill_states() {
        // All combinations of active/editable
        let inactive_readonly = Pill {
            label: "A".to_string(),
            value: "1".to_string(),
            active: false,
            editable: false,
        };
        assert!(!inactive_readonly.active && !inactive_readonly.editable);

        let inactive_editable = Pill {
            label: "B".to_string(),
            value: "2".to_string(),
            active: false,
            editable: true,
        };
        assert!(!inactive_editable.active && inactive_editable.editable);

        let active_readonly = Pill {
            label: "C".to_string(),
            value: "3".to_string(),
            active: true,
            editable: false,
        };
        assert!(active_readonly.active && !active_readonly.editable);

        let active_editable = Pill {
            label: "D".to_string(),
            value: "4".to_string(),
            active: true,
            editable: true,
        };
        assert!(active_editable.active && active_editable.editable);
    }
}
