use ratatui::layout::Alignment;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::ui::components::theme::ThemePalette;
use ratatui::widgets::Wrap;

pub fn search_bar(
    query: &str,
    palette: ThemePalette,
    focused: bool,
    mode_label: &str,
    chips: Vec<Span<'static>>,
) -> Paragraph<'static> {
    let title = Span::styled(format!("Search · {mode_label}"), palette.title());
    let style = if focused {
        Style::default().fg(palette.accent)
    } else {
        Style::default().fg(palette.hint)
    };

    let mut first_line = chips;
    if !first_line.is_empty() {
        first_line.push(Span::raw(" "));
    }
    first_line.push(Span::styled(format!("/ {}", query), style));

    let body = vec![
        Line::from(first_line),
        Line::from(vec![
            Span::styled("Tips: ", palette.title()),
            Span::raw(
                "F3 agent • F4 workspace • F5/F6 time • F7 context • F11 clear • F9 mode • F2 theme • F8/Enter open • Ctrl-R history",
            ),
        ]),
    ];

    Paragraph::new(body)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.accent_alt)),
        )
        .style(Style::default())
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
}
