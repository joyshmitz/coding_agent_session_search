//! FrankenTUI style-system scaffolding for cass.
//!
//! Centralizes:
//! - theme preset selection
//! - color profile downgrade (mono / ansi16 / ansi256 / truecolor)
//! - env opt-outs (`NO_COLOR`, `CASS_NO_COLOR`, `CASS_NO_ICONS`, `CASS_NO_GRADIENT`)
//! - accessibility text markers (`CASS_A11Y`)
//! - semantic `StyleSheet` tokens used by upcoming ftui views

use ftui::render::cell::PackedRgba;
use ftui::style::theme::themes;
use ftui::{
    AdaptiveColor, Color, ColorProfile, ResolvedTheme, Style, StyleSheet, Theme, ThemeBuilder,
};

pub const STYLE_APP_ROOT: &str = "app.root";
pub const STYLE_PANE_BASE: &str = "pane.base";
pub const STYLE_PANE_FOCUSED: &str = "pane.focused";
pub const STYLE_TEXT_PRIMARY: &str = "text.primary";
pub const STYLE_TEXT_MUTED: &str = "text.muted";
pub const STYLE_TEXT_SUBTLE: &str = "text.subtle";
pub const STYLE_STATUS_SUCCESS: &str = "status.success";
pub const STYLE_STATUS_WARNING: &str = "status.warning";
pub const STYLE_STATUS_ERROR: &str = "status.error";
pub const STYLE_STATUS_INFO: &str = "status.info";
pub const STYLE_RESULT_ROW: &str = "result.row";
pub const STYLE_RESULT_ROW_ALT: &str = "result.row.alt";
pub const STYLE_RESULT_ROW_SELECTED: &str = "result.row.selected";
pub const STYLE_ROLE_USER: &str = "role.user";
pub const STYLE_ROLE_ASSISTANT: &str = "role.assistant";
pub const STYLE_ROLE_TOOL: &str = "role.tool";
pub const STYLE_ROLE_SYSTEM: &str = "role.system";
pub const STYLE_ROLE_GUTTER_USER: &str = "role.gutter.user";
pub const STYLE_ROLE_GUTTER_ASSISTANT: &str = "role.gutter.assistant";
pub const STYLE_ROLE_GUTTER_TOOL: &str = "role.gutter.tool";
pub const STYLE_ROLE_GUTTER_SYSTEM: &str = "role.gutter.system";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UiThemePreset {
    #[default]
    Dark,
    Light,
    HighContrast,
    Catppuccin,
    Dracula,
    Nord,
}

impl UiThemePreset {
    pub const fn all() -> [Self; 6] {
        [
            Self::Dark,
            Self::Light,
            Self::Catppuccin,
            Self::Dracula,
            Self::Nord,
            Self::HighContrast,
        ]
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Dark => "Dark",
            Self::Light => "Light",
            Self::HighContrast => "High Contrast",
            Self::Catppuccin => "Catppuccin",
            Self::Dracula => "Dracula",
            Self::Nord => "Nord",
        }
    }

    pub fn next(self) -> Self {
        let all = Self::all();
        let idx = all.iter().position(|preset| *preset == self).unwrap_or(0);
        all[(idx + 1) % all.len()]
    }

    pub fn previous(self) -> Self {
        let all = Self::all();
        let idx = all.iter().position(|preset| *preset == self).unwrap_or(0);
        all[(idx + all.len() - 1) % all.len()]
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dark" => Some(Self::Dark),
            "light" => Some(Self::Light),
            "high-contrast" | "high_contrast" | "highcontrast" | "hc" => Some(Self::HighContrast),
            "catppuccin" | "cat" => Some(Self::Catppuccin),
            "dracula" => Some(Self::Dracula),
            "nord" => Some(Self::Nord),
            _ => None,
        }
    }

    fn base_theme(self) -> Theme {
        match self {
            Self::Dark => themes::dark(),
            Self::Light => themes::light(),
            Self::HighContrast => high_contrast_theme(),
            Self::Catppuccin => catppuccin_theme(),
            Self::Dracula => themes::dracula(),
            Self::Nord => themes::nord(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StyleOptions {
    pub preset: UiThemePreset,
    pub dark_mode: bool,
    pub color_profile: ColorProfile,
    pub no_color: bool,
    pub no_icons: bool,
    pub no_gradient: bool,
    pub a11y: bool,
}

impl Default for StyleOptions {
    fn default() -> Self {
        Self {
            preset: UiThemePreset::Dark,
            dark_mode: true,
            color_profile: ColorProfile::detect(),
            no_color: false,
            no_icons: false,
            no_gradient: false,
            a11y: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct EnvValues<'a> {
    no_color: Option<&'a str>,
    cass_no_color: Option<&'a str>,
    colorterm: Option<&'a str>,
    term: Option<&'a str>,
    cass_no_icons: Option<&'a str>,
    cass_no_gradient: Option<&'a str>,
    cass_a11y: Option<&'a str>,
    cass_theme: Option<&'a str>,
    cass_color_profile: Option<&'a str>,
}

impl StyleOptions {
    pub fn from_env() -> Self {
        let no_color = dotenvy::var("NO_COLOR").ok();
        let cass_no_color = dotenvy::var("CASS_NO_COLOR").ok();
        let colorterm = dotenvy::var("COLORTERM").ok();
        let term = dotenvy::var("TERM").ok();
        let cass_no_icons = dotenvy::var("CASS_NO_ICONS").ok();
        let cass_no_gradient = dotenvy::var("CASS_NO_GRADIENT").ok();
        let cass_a11y = dotenvy::var("CASS_A11Y").ok();
        let cass_theme = dotenvy::var("CASS_THEME").ok();
        let cass_color_profile = dotenvy::var("CASS_COLOR_PROFILE").ok();

        Self::from_env_values(EnvValues {
            no_color: no_color.as_deref(),
            cass_no_color: cass_no_color.as_deref(),
            colorterm: colorterm.as_deref(),
            term: term.as_deref(),
            cass_no_icons: cass_no_icons.as_deref(),
            cass_no_gradient: cass_no_gradient.as_deref(),
            cass_a11y: cass_a11y.as_deref(),
            cass_theme: cass_theme.as_deref(),
            cass_color_profile: cass_color_profile.as_deref(),
        })
    }

    fn from_env_values(values: EnvValues<'_>) -> Self {
        let preset = values
            .cass_theme
            .and_then(UiThemePreset::parse)
            .unwrap_or(UiThemePreset::Dark);

        let no_color_enabled = values.no_color.is_some() || values.cass_no_color.is_some();

        let detected_profile = ColorProfile::detect_from_env(None, values.colorterm, values.term);
        let profile_override = values.cass_color_profile.and_then(parse_color_profile);
        let color_profile = if no_color_enabled {
            ColorProfile::Mono
        } else {
            profile_override.unwrap_or(detected_profile)
        };

        let a11y = env_truthy(values.cass_a11y);
        let no_icons = values.cass_no_icons.is_some();
        let no_gradient = values.cass_no_gradient.is_some() || no_color_enabled || a11y;

        let dark_mode = match preset {
            UiThemePreset::Light => false,
            UiThemePreset::HighContrast => Theme::detect_dark_mode(),
            _ => true,
        };

        Self {
            preset,
            dark_mode,
            color_profile,
            no_color: no_color_enabled,
            no_icons,
            no_gradient,
            a11y,
        }
    }

    pub const fn gradients_enabled(self) -> bool {
        !self.no_gradient && self.color_profile.supports_color()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleMarkers {
    pub user: &'static str,
    pub assistant: &'static str,
    pub tool: &'static str,
    pub system: &'static str,
}

impl RoleMarkers {
    fn from_options(options: StyleOptions) -> Self {
        if options.a11y {
            return Self {
                user: "[user]",
                assistant: "[assistant]",
                tool: "[tool]",
                system: "[system]",
            };
        }

        if options.no_icons {
            return Self {
                user: "",
                assistant: "",
                tool: "",
                system: "",
            };
        }

        Self {
            user: "U>",
            assistant: "A>",
            tool: "T>",
            system: "S>",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StyleContext {
    pub options: StyleOptions,
    pub theme: Theme,
    pub resolved: ResolvedTheme,
    pub sheet: StyleSheet,
    pub role_markers: RoleMarkers,
}

impl StyleContext {
    pub fn from_options(options: StyleOptions) -> Self {
        let mut theme = options.preset.base_theme();

        if options.a11y && options.preset != UiThemePreset::HighContrast {
            theme = apply_a11y_overrides(theme);
        }

        theme = downgrade_theme_for_profile(theme, options.color_profile);

        let dark_mode = if options.preset == UiThemePreset::Light {
            false
        } else {
            options.dark_mode
        };
        let resolved = theme.resolve(dark_mode);
        let sheet = build_stylesheet(resolved, options);
        let role_markers = RoleMarkers::from_options(options);

        Self {
            options,
            theme,
            resolved,
            sheet,
            role_markers,
        }
    }

    pub fn from_env() -> Self {
        Self::from_options(StyleOptions::from_env())
    }

    pub fn style(&self, name: &str) -> Style {
        self.sheet.get_or_default(name)
    }
}

fn parse_color_profile(value: &str) -> Option<ColorProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mono" | "none" => Some(ColorProfile::Mono),
        "ansi16" | "16" => Some(ColorProfile::Ansi16),
        "ansi256" | "256" => Some(ColorProfile::Ansi256),
        "truecolor" | "24bit" | "rgb" => Some(ColorProfile::TrueColor),
        _ => None,
    }
}

fn env_truthy(value: Option<&str>) -> bool {
    match value {
        Some(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            !(normalized == "0"
                || normalized == "false"
                || normalized == "off"
                || normalized == "no")
        }
        None => false,
    }
}

fn catppuccin_theme() -> Theme {
    ThemeBuilder::from_theme(themes::dark())
        .primary(Color::rgb(137, 180, 250))
        .secondary(Color::rgb(245, 194, 231))
        .accent(Color::rgb(203, 166, 247))
        .background(Color::rgb(30, 30, 46))
        .surface(Color::rgb(49, 50, 68))
        .overlay(Color::rgb(69, 71, 90))
        .text(Color::rgb(205, 214, 244))
        .text_muted(Color::rgb(166, 173, 200))
        .text_subtle(Color::rgb(127, 132, 156))
        .success(Color::rgb(166, 227, 161))
        .warning(Color::rgb(249, 226, 175))
        .error(Color::rgb(243, 139, 168))
        .info(Color::rgb(137, 220, 235))
        .border(Color::rgb(88, 91, 112))
        .border_focused(Color::rgb(180, 190, 254))
        .selection_bg(Color::rgb(116, 199, 236))
        .selection_fg(Color::rgb(30, 30, 46))
        .scrollbar_track(Color::rgb(49, 50, 68))
        .scrollbar_thumb(Color::rgb(88, 91, 112))
        .build()
}

fn high_contrast_theme() -> Theme {
    ThemeBuilder::from_theme(themes::dark())
        .primary(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .secondary(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .accent(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 0),
        ))
        .background(AdaptiveColor::adaptive(
            Color::rgb(255, 255, 255),
            Color::rgb(0, 0, 0),
        ))
        .surface(AdaptiveColor::adaptive(
            Color::rgb(245, 245, 245),
            Color::rgb(0, 0, 0),
        ))
        .overlay(AdaptiveColor::adaptive(
            Color::rgb(235, 235, 235),
            Color::rgb(0, 0, 0),
        ))
        .text(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .text_muted(AdaptiveColor::adaptive(
            Color::rgb(30, 30, 30),
            Color::rgb(215, 215, 215),
        ))
        .text_subtle(AdaptiveColor::adaptive(
            Color::rgb(45, 45, 45),
            Color::rgb(200, 200, 200),
        ))
        .success(AdaptiveColor::adaptive(
            Color::rgb(0, 96, 0),
            Color::rgb(64, 255, 64),
        ))
        .warning(AdaptiveColor::adaptive(
            Color::rgb(110, 70, 0),
            Color::rgb(255, 220, 64),
        ))
        .error(AdaptiveColor::adaptive(
            Color::rgb(128, 0, 0),
            Color::rgb(255, 96, 96),
        ))
        .info(AdaptiveColor::adaptive(
            Color::rgb(0, 32, 128),
            Color::rgb(128, 200, 255),
        ))
        .border(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .border_focused(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 0),
        ))
        .selection_bg(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .selection_fg(AdaptiveColor::adaptive(
            Color::rgb(255, 255, 255),
            Color::rgb(0, 0, 0),
        ))
        .scrollbar_track(AdaptiveColor::adaptive(
            Color::rgb(235, 235, 235),
            Color::rgb(0, 0, 0),
        ))
        .scrollbar_thumb(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .build()
}

fn apply_a11y_overrides(theme: Theme) -> Theme {
    ThemeBuilder::from_theme(theme)
        .border_focused(Color::rgb(255, 255, 0))
        .selection_bg(AdaptiveColor::adaptive(
            Color::rgb(0, 0, 0),
            Color::rgb(255, 255, 255),
        ))
        .selection_fg(AdaptiveColor::adaptive(
            Color::rgb(255, 255, 255),
            Color::rgb(0, 0, 0),
        ))
        .build()
}

fn downgrade_adaptive_color(color: AdaptiveColor, profile: ColorProfile) -> AdaptiveColor {
    match color {
        AdaptiveColor::Fixed(value) => AdaptiveColor::fixed(value.downgrade(profile)),
        AdaptiveColor::Adaptive { light, dark } => {
            AdaptiveColor::adaptive(light.downgrade(profile), dark.downgrade(profile))
        }
    }
}

fn downgrade_theme_for_profile(theme: Theme, profile: ColorProfile) -> Theme {
    if profile == ColorProfile::TrueColor {
        return theme;
    }

    Theme {
        primary: downgrade_adaptive_color(theme.primary, profile),
        secondary: downgrade_adaptive_color(theme.secondary, profile),
        accent: downgrade_adaptive_color(theme.accent, profile),
        background: downgrade_adaptive_color(theme.background, profile),
        surface: downgrade_adaptive_color(theme.surface, profile),
        overlay: downgrade_adaptive_color(theme.overlay, profile),
        text: downgrade_adaptive_color(theme.text, profile),
        text_muted: downgrade_adaptive_color(theme.text_muted, profile),
        text_subtle: downgrade_adaptive_color(theme.text_subtle, profile),
        success: downgrade_adaptive_color(theme.success, profile),
        warning: downgrade_adaptive_color(theme.warning, profile),
        error: downgrade_adaptive_color(theme.error, profile),
        info: downgrade_adaptive_color(theme.info, profile),
        border: downgrade_adaptive_color(theme.border, profile),
        border_focused: downgrade_adaptive_color(theme.border_focused, profile),
        selection_bg: downgrade_adaptive_color(theme.selection_bg, profile),
        selection_fg: downgrade_adaptive_color(theme.selection_fg, profile),
        scrollbar_track: downgrade_adaptive_color(theme.scrollbar_track, profile),
        scrollbar_thumb: downgrade_adaptive_color(theme.scrollbar_thumb, profile),
    }
}

fn build_stylesheet(resolved: ResolvedTheme, options: StyleOptions) -> StyleSheet {
    let sheet = StyleSheet::new();

    let zebra_bg = if options.gradients_enabled() {
        blend(resolved.surface, resolved.background, 0.35).downgrade(options.color_profile)
    } else {
        resolved.surface
    };

    let role_user = resolved.primary;
    let role_assistant = resolved.info;
    let role_tool = resolved.warning;
    let role_system = resolved.error;

    sheet.define(
        STYLE_APP_ROOT,
        Style::new()
            .fg(to_packed(resolved.text))
            .bg(to_packed(resolved.background)),
    );
    sheet.define(
        STYLE_PANE_BASE,
        Style::new()
            .fg(to_packed(resolved.text))
            .bg(to_packed(resolved.surface)),
    );
    sheet.define(
        STYLE_PANE_FOCUSED,
        Style::new()
            .fg(to_packed(resolved.text))
            .bg(to_packed(resolved.surface))
            .underline(),
    );

    sheet.define(
        STYLE_TEXT_PRIMARY,
        Style::new().fg(to_packed(resolved.text)),
    );
    sheet.define(
        STYLE_TEXT_MUTED,
        Style::new().fg(to_packed(resolved.text_muted)),
    );
    sheet.define(
        STYLE_TEXT_SUBTLE,
        Style::new().fg(to_packed(resolved.text_subtle)),
    );

    sheet.define(
        STYLE_STATUS_SUCCESS,
        Style::new().fg(to_packed(resolved.success)).bold(),
    );
    sheet.define(
        STYLE_STATUS_WARNING,
        Style::new().fg(to_packed(resolved.warning)).bold(),
    );
    sheet.define(
        STYLE_STATUS_ERROR,
        Style::new().fg(to_packed(resolved.error)).bold(),
    );
    sheet.define(
        STYLE_STATUS_INFO,
        Style::new().fg(to_packed(resolved.info)).bold(),
    );

    sheet.define(
        STYLE_RESULT_ROW,
        Style::new()
            .fg(to_packed(resolved.text))
            .bg(to_packed(resolved.surface)),
    );
    sheet.define(
        STYLE_RESULT_ROW_ALT,
        Style::new()
            .fg(to_packed(resolved.text))
            .bg(to_packed(zebra_bg)),
    );

    let selected_style = if options.a11y {
        Style::new()
            .fg(to_packed(resolved.selection_fg))
            .bg(to_packed(resolved.selection_bg))
            .bold()
            .underline()
    } else {
        Style::new()
            .fg(to_packed(resolved.selection_fg))
            .bg(to_packed(resolved.selection_bg))
            .bold()
    };
    sheet.define(STYLE_RESULT_ROW_SELECTED, selected_style);

    let role_user_style = if options.a11y {
        Style::new().fg(to_packed(role_user)).bold().underline()
    } else {
        Style::new().fg(to_packed(role_user)).bold()
    };
    let role_assistant_style = if options.a11y {
        Style::new().fg(to_packed(role_assistant)).bold().italic()
    } else {
        Style::new().fg(to_packed(role_assistant)).bold()
    };
    let role_tool_style = if options.a11y {
        Style::new().fg(to_packed(role_tool)).underline()
    } else {
        Style::new().fg(to_packed(role_tool))
    };
    let role_system_style = if options.a11y {
        Style::new().fg(to_packed(role_system)).bold().underline()
    } else {
        Style::new().fg(to_packed(role_system)).bold()
    };

    sheet.define(STYLE_ROLE_USER, role_user_style);
    sheet.define(STYLE_ROLE_ASSISTANT, role_assistant_style);
    sheet.define(STYLE_ROLE_TOOL, role_tool_style);
    sheet.define(STYLE_ROLE_SYSTEM, role_system_style);

    sheet.define(
        STYLE_ROLE_GUTTER_USER,
        Style::new().fg(to_packed(role_user)).bg(to_packed(blend(
            resolved.background,
            role_user,
            0.18,
        ))),
    );
    sheet.define(
        STYLE_ROLE_GUTTER_ASSISTANT,
        Style::new()
            .fg(to_packed(role_assistant))
            .bg(to_packed(blend(resolved.background, role_assistant, 0.18))),
    );
    sheet.define(
        STYLE_ROLE_GUTTER_TOOL,
        Style::new().fg(to_packed(role_tool)).bg(to_packed(blend(
            resolved.background,
            role_tool,
            0.18,
        ))),
    );
    sheet.define(
        STYLE_ROLE_GUTTER_SYSTEM,
        Style::new().fg(to_packed(role_system)).bg(to_packed(blend(
            resolved.background,
            role_system,
            0.18,
        ))),
    );

    sheet
}

fn to_packed(color: Color) -> PackedRgba {
    let rgb = color.to_rgb();
    PackedRgba::rgb(rgb.r, rgb.g, rgb.b)
}

fn blend(a: Color, b: Color, t: f32) -> Color {
    let t = t.clamp(0.0, 1.0);
    let ar = a.to_rgb();
    let br = b.to_rgb();

    let blend_channel = |left: u8, right: u8| -> u8 {
        let mixed = left as f32 + (right as f32 - left as f32) * t;
        mixed.round().clamp(0.0, 255.0) as u8
    };

    Color::rgb(
        blend_channel(ar.r, br.r),
        blend_channel(ar.g, br.g),
        blend_channel(ar.b, br.b),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preset_parse_and_cycles_are_stable() {
        assert_eq!(UiThemePreset::parse("dark"), Some(UiThemePreset::Dark));
        assert_eq!(UiThemePreset::parse("light"), Some(UiThemePreset::Light));
        assert_eq!(
            UiThemePreset::parse("catppuccin"),
            Some(UiThemePreset::Catppuccin)
        );
        assert_eq!(
            UiThemePreset::parse("dracula"),
            Some(UiThemePreset::Dracula)
        );
        assert_eq!(UiThemePreset::parse("nord"), Some(UiThemePreset::Nord));
        assert_eq!(
            UiThemePreset::parse("high_contrast"),
            Some(UiThemePreset::HighContrast)
        );

        assert_eq!(UiThemePreset::Dark.next(), UiThemePreset::Light);
        assert_eq!(UiThemePreset::Light.previous(), UiThemePreset::Dark);
        assert_eq!(UiThemePreset::Dark.previous(), UiThemePreset::HighContrast);
    }

    #[test]
    fn options_from_values_honor_opt_out_and_profile_override() {
        let options = StyleOptions::from_env_values(EnvValues {
            no_color: Some("1"),
            cass_no_color: None,
            colorterm: Some("truecolor"),
            term: Some("xterm-256color"),
            cass_no_icons: Some("1"),
            cass_no_gradient: Some("1"),
            cass_a11y: Some("true"),
            cass_theme: Some("nord"),
            cass_color_profile: Some("ansi16"),
        });

        assert_eq!(options.preset, UiThemePreset::Nord);
        assert!(options.no_color);
        assert!(options.no_icons);
        assert!(options.no_gradient);
        assert!(options.a11y);
        assert_eq!(options.color_profile, ColorProfile::Mono);
    }

    #[test]
    fn options_profile_override_applies_when_color_enabled() {
        let options = StyleOptions::from_env_values(EnvValues {
            no_color: None,
            cass_no_color: None,
            colorterm: Some("truecolor"),
            term: Some("xterm-256color"),
            cass_no_icons: None,
            cass_no_gradient: None,
            cass_a11y: Some("0"),
            cass_theme: Some("dark"),
            cass_color_profile: Some("ansi16"),
        });

        assert_eq!(options.color_profile, ColorProfile::Ansi16);
        assert!(!options.no_color);
    }

    #[test]
    fn style_context_builds_required_semantic_styles() {
        let context = StyleContext::from_options(StyleOptions {
            preset: UiThemePreset::Dark,
            dark_mode: true,
            color_profile: ColorProfile::TrueColor,
            no_color: false,
            no_icons: false,
            no_gradient: false,
            a11y: false,
        });

        for key in [
            STYLE_APP_ROOT,
            STYLE_PANE_BASE,
            STYLE_PANE_FOCUSED,
            STYLE_RESULT_ROW,
            STYLE_RESULT_ROW_ALT,
            STYLE_RESULT_ROW_SELECTED,
            STYLE_ROLE_USER,
            STYLE_ROLE_ASSISTANT,
            STYLE_ROLE_TOOL,
            STYLE_ROLE_SYSTEM,
            STYLE_ROLE_GUTTER_USER,
            STYLE_ROLE_GUTTER_ASSISTANT,
            STYLE_ROLE_GUTTER_TOOL,
            STYLE_ROLE_GUTTER_SYSTEM,
        ] {
            assert!(context.sheet.contains(key), "missing style token: {key}");
        }
    }

    #[test]
    fn mono_profile_downgrades_theme_colors() {
        let context = StyleContext::from_options(StyleOptions {
            preset: UiThemePreset::Dracula,
            dark_mode: true,
            color_profile: ColorProfile::Mono,
            no_color: true,
            no_icons: false,
            no_gradient: true,
            a11y: false,
        });

        assert!(matches!(context.resolved.primary, Color::Mono(_)));
        assert!(matches!(context.resolved.background, Color::Mono(_)));
        assert!(matches!(context.resolved.text, Color::Mono(_)));
    }

    #[test]
    fn accessibility_role_markers_prioritize_text_labels() {
        let markers = RoleMarkers::from_options(StyleOptions {
            preset: UiThemePreset::Dark,
            dark_mode: true,
            color_profile: ColorProfile::Ansi256,
            no_color: false,
            no_icons: true,
            no_gradient: true,
            a11y: true,
        });

        assert_eq!(markers.user, "[user]");
        assert_eq!(markers.assistant, "[assistant]");
        assert_eq!(markers.tool, "[tool]");
        assert_eq!(markers.system, "[system]");
    }

    #[test]
    fn base_contrast_is_wcag_aa_or_higher_for_all_presets() {
        for preset in UiThemePreset::all() {
            let dark_mode = !matches!(preset, UiThemePreset::Light);
            let context = StyleContext::from_options(StyleOptions {
                preset,
                dark_mode,
                color_profile: ColorProfile::TrueColor,
                no_color: false,
                no_icons: false,
                no_gradient: false,
                a11y: false,
            });

            let root = context.style(STYLE_APP_ROOT);
            let fg = root.fg.expect("app.root must define foreground");
            let bg = root.bg.expect("app.root must define background");
            let ratio = ftui::style::contrast_ratio_packed(fg, bg);
            assert!(
                ratio >= 4.5,
                "contrast too low for {}: {ratio}",
                preset.name()
            );
        }
    }

    #[test]
    fn high_contrast_preset_keeps_selection_legible() {
        let context = StyleContext::from_options(StyleOptions {
            preset: UiThemePreset::HighContrast,
            dark_mode: true,
            color_profile: ColorProfile::Ansi16,
            no_color: false,
            no_icons: false,
            no_gradient: true,
            a11y: true,
        });

        let selected = context.style(STYLE_RESULT_ROW_SELECTED);
        let fg = selected
            .fg
            .expect("selected row style should define foreground color");
        let bg = selected
            .bg
            .expect("selected row style should define background color");

        let ratio = ftui::style::contrast_ratio_packed(fg, bg);
        assert!(ratio >= 4.5, "selected row contrast too low: {ratio}");
    }
}
