//! Theme definitions.

use ratatui::style::{Color, Modifier, Style};

#[derive(Clone, Copy)]
pub struct PaneTheme {
    pub bg: Color,
    pub fg: Color,
    pub accent: Color,
}

#[derive(Clone, Copy)]
pub struct ThemePalette {
    pub accent: Color,
    pub accent_alt: Color,
    pub bg: Color,
    pub fg: Color,
    pub surface: Color,
    pub hint: Color,
    pub user: Color,
    pub agent: Color,
    pub tool: Color,
    pub system: Color,
}

impl ThemePalette {
    pub fn light() -> Self {
        Self {
            accent: Color::Cyan,
            accent_alt: Color::LightBlue,
            bg: Color::White,
            fg: Color::Black,
            surface: Color::Gray,
            hint: Color::DarkGray,
            user: Color::Green,
            agent: Color::Blue,
            tool: Color::Magenta,
            system: Color::Yellow,
        }
    }

    pub fn dark() -> Self {
        Self {
            accent: Color::Cyan,
            accent_alt: Color::Blue,
            bg: Color::Black,
            fg: Color::White,
            surface: Color::DarkGray,
            hint: Color::Gray,
            user: Color::Green,
            agent: Color::Cyan,
            tool: Color::Magenta,
            system: Color::Yellow,
        }
    }

    pub fn title(self) -> Style {
        Style::default()
            .fg(self.accent)
            .add_modifier(Modifier::BOLD)
    }

    /// Fixed per-agent pane colors so each tool is instantly recognizable.
    ///
    /// We intentionally keep dark backgrounds and vivid accents for readability
    /// regardless of the global light/dark toggle.
    pub fn agent_pane(agent: &str) -> PaneTheme {
        let slug = agent.to_lowercase().replace('-', "_");
        match slug.as_str() {
            "claude_code" | "claude" => PaneTheme {
                bg: Color::Rgb(8, 24, 80),
                fg: Color::Yellow,
                accent: Color::LightYellow,
            },
            "codex" => PaneTheme {
                bg: Color::Rgb(4, 40, 18),
                fg: Color::LightMagenta,
                accent: Color::Magenta,
            },
            "cline" => PaneTheme {
                bg: Color::Rgb(10, 48, 58),
                fg: Color::White,
                accent: Color::LightCyan,
            },
            "gemini" | "gemini_cli" => PaneTheme {
                bg: Color::Rgb(54, 0, 70),
                fg: Color::LightCyan,
                accent: Color::Cyan,
            },
            "amp" => PaneTheme {
                bg: Color::Rgb(70, 8, 8),
                fg: Color::White,
                accent: Color::LightRed,
            },
            "opencode" => PaneTheme {
                bg: Color::Rgb(32, 32, 36),
                fg: Color::LightGreen,
                accent: Color::Green,
            },
            _ => PaneTheme {
                bg: Color::Black,
                fg: Color::White,
                accent: Color::Cyan,
            },
        }
    }
}
