//! Breadcrumb bar component for the TUI.
//! Displays current context (Agent › Workspace › Date) and ranking.
//! Interactive elements allow direct clearing/changing of filters.

use ratatui::prelude::*;
use ratatui::widgets::{Block, Paragraph};

use crate::search::query::SearchFilters;
use crate::ui::components::theme::ThemePalette;
use crate::ui::tui;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreadcrumbKind {
    Agent,
    Workspace,
    Date,
    Ranking,
    None,
}

fn ranking_label(r: tui::RankingMode) -> &'static str {
    match r {
        tui::RankingMode::RecentHeavy => "Recent",
        tui::RankingMode::Balanced => "Balanced",
        tui::RankingMode::RelevanceHeavy => "Relevance",
        tui::RankingMode::MatchQualityHeavy => "Quality",
        tui::RankingMode::DateNewest => "Newest",
        tui::RankingMode::DateOldest => "Oldest",
    }
}

pub fn render_breadcrumbs(
    f: &mut Frame,
    area: Rect,
    filters: &SearchFilters,
    ranking: tui::RankingMode,
    palette: ThemePalette,
) -> Vec<(Rect, BreadcrumbKind)> {
    let mut rects: Vec<(Rect, BreadcrumbKind)> = Vec::new();
    let mut spans: Vec<Span> = Vec::new();

    // Helper to add a separator
    let add_sep = |spans: &mut Vec<Span>| {
        spans.push(Span::styled(" › ", Style::default().fg(palette.hint)));
    };

    // 1. Agent
    let agent_text = if filters.agents.is_empty() {
        "All agents".to_string()
    } else {
        let mut list: Vec<_> = filters.agents.iter().cloned().collect();
        list.sort();
        if list.len() > 3 {
            format!("{}, {} +{}", list[0], list[1], list.len() - 2)
        } else {
            list.join(",")
        }
    };
    let agent_style = if filters.agents.is_empty() {
        Style::default().fg(palette.hint)
    } else {
        Style::default()
            .fg(palette.accent)
            .add_modifier(Modifier::BOLD)
    };
    spans.push(Span::styled(agent_text.clone(), agent_style));
    // Simplified rect tracking: we just map the whole area for now since individual click
    // handling requires precise text measurement which Paragraph doesn't easily expose
    // in a way that maps 1:1 to screen coordinates without manual layout.
    // For this iteration, we'll render the text and return generic hit areas if needed,
    // or rely on the caller to handle general interaction.
    // However, to support "crumb choosers", we really want distinct zones.
    // Let's approximate width based on display width (not just char count).
    use unicode_width::UnicodeWidthStr;

    let mut current_x = area.x;
    let measure_width = |s: &str| s.width() as u16;

    let agent_width = measure_width(&agent_text);
    rects.push((
        Rect::new(current_x, area.y, agent_width, 1),
        BreadcrumbKind::Agent,
    ));
    current_x += agent_width + 3; // " › " is 3 chars

    add_sep(&mut spans);

    // 2. Workspace
    let ws_text = if filters.workspaces.is_empty() {
        "All workspaces".to_string()
    } else {
        let mut list: Vec<_> = filters.workspaces.iter().cloned().collect();
        list.sort();
        if list.len() > 2 {
            format!("{}, +{}", list[0], list.len() - 1)
        } else {
            list.join(",")
        }
    };
    let ws_style = if filters.workspaces.is_empty() {
        Style::default().fg(palette.hint)
    } else {
        Style::default()
            .fg(palette.accent)
            .add_modifier(Modifier::BOLD)
    };
    spans.push(Span::styled(ws_text.clone(), ws_style));
    let ws_width = measure_width(&ws_text);
    rects.push((
        Rect::new(current_x, area.y, ws_width, 1),
        BreadcrumbKind::Workspace,
    ));
    current_x += ws_width + 3;

    add_sep(&mut spans);

    // 3. Date
    let date_text = match (filters.created_from, filters.created_to) {
        (None, None) => "Any time".to_string(),
        (Some(f), None) => format!("Since {}", tui::format_time_short(f)),
        (None, Some(t)) => format!("Until {}", tui::format_time_short(t)),
        (Some(f), Some(t)) => format!(
            "{} - {}",
            tui::format_time_short(f),
            tui::format_time_short(t)
        ),
    };
    let date_style = if filters.created_from.is_none() && filters.created_to.is_none() {
        Style::default().fg(palette.hint)
    } else {
        Style::default()
            .fg(palette.accent)
            .add_modifier(Modifier::BOLD)
    };
    spans.push(Span::styled(date_text.clone(), date_style));
    let date_width = measure_width(&date_text);
    rects.push((
        Rect::new(current_x, area.y, date_width, 1),
        BreadcrumbKind::Date,
    ));
    current_x += date_width + 3;

    add_sep(&mut spans);

    // 4. Ranking
    let rank_text = ranking_label(ranking);
    // Ranking is always active, so we use a distinct color but maybe not bold unless changed?
    // Let's keep it subtle but distinct.
    spans.push(Span::styled(rank_text, Style::default().fg(palette.fg)));
    let rank_width = measure_width(rank_text);
    rects.push((
        Rect::new(current_x, area.y, rank_width, 1),
        BreadcrumbKind::Ranking,
    ));

    // Render
    let para = Paragraph::new(Line::from(spans))
        .block(Block::default().style(Style::default().bg(palette.bg)));
    f.render_widget(para, area);

    rects
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== BreadcrumbKind tests ====================

    #[test]
    fn test_breadcrumb_kind_equality() {
        assert_eq!(BreadcrumbKind::Agent, BreadcrumbKind::Agent);
        assert_eq!(BreadcrumbKind::Workspace, BreadcrumbKind::Workspace);
        assert_eq!(BreadcrumbKind::Date, BreadcrumbKind::Date);
        assert_eq!(BreadcrumbKind::Ranking, BreadcrumbKind::Ranking);
        assert_eq!(BreadcrumbKind::None, BreadcrumbKind::None);
    }

    #[test]
    fn test_breadcrumb_kind_inequality() {
        assert_ne!(BreadcrumbKind::Agent, BreadcrumbKind::Workspace);
        assert_ne!(BreadcrumbKind::Date, BreadcrumbKind::Ranking);
        assert_ne!(BreadcrumbKind::None, BreadcrumbKind::Agent);
    }

    #[test]
    fn test_breadcrumb_kind_clone() {
        let kind = BreadcrumbKind::Agent;
        let cloned = kind;
        assert_eq!(kind, cloned);
    }

    #[test]
    fn test_breadcrumb_kind_copy() {
        let kind = BreadcrumbKind::Workspace;
        let copied: BreadcrumbKind = kind;
        assert_eq!(copied, BreadcrumbKind::Workspace);
    }

    #[test]
    fn test_breadcrumb_kind_debug() {
        let debug_str = format!("{:?}", BreadcrumbKind::Agent);
        assert!(debug_str.contains("Agent"));

        let debug_str = format!("{:?}", BreadcrumbKind::None);
        assert!(debug_str.contains("None"));
    }

    // ==================== ranking_label tests ====================

    #[test]
    fn test_ranking_label_recent_heavy() {
        assert_eq!(ranking_label(tui::RankingMode::RecentHeavy), "Recent");
    }

    #[test]
    fn test_ranking_label_balanced() {
        assert_eq!(ranking_label(tui::RankingMode::Balanced), "Balanced");
    }

    #[test]
    fn test_ranking_label_relevance_heavy() {
        assert_eq!(ranking_label(tui::RankingMode::RelevanceHeavy), "Relevance");
    }

    #[test]
    fn test_ranking_label_match_quality_heavy() {
        assert_eq!(
            ranking_label(tui::RankingMode::MatchQualityHeavy),
            "Quality"
        );
    }

    #[test]
    fn test_ranking_label_date_newest() {
        assert_eq!(ranking_label(tui::RankingMode::DateNewest), "Newest");
    }

    #[test]
    fn test_ranking_label_date_oldest() {
        assert_eq!(ranking_label(tui::RankingMode::DateOldest), "Oldest");
    }

    #[test]
    fn test_ranking_label_all_modes_non_empty() {
        let modes = [
            tui::RankingMode::RecentHeavy,
            tui::RankingMode::Balanced,
            tui::RankingMode::RelevanceHeavy,
            tui::RankingMode::MatchQualityHeavy,
            tui::RankingMode::DateNewest,
            tui::RankingMode::DateOldest,
        ];

        for mode in modes {
            let label = ranking_label(mode);
            assert!(
                !label.is_empty(),
                "Label for {:?} should not be empty",
                mode
            );
        }
    }
}
