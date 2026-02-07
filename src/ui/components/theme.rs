//! Premium theme definitions with world-class, Stripe-level aesthetics.
//!
//! Design principles:
//! - Muted, sophisticated colors that are easy on the eyes
//! - Clear visual hierarchy with accent colors used sparingly
//! - Consistent design language across all elements
//! - High contrast where it matters (text legibility)
//! - Subtle agent differentiation via tinted backgrounds

use ftui::render::cell::PackedRgba;
use ftui::Style;

/// Premium color palette inspired by modern design systems.
/// Uses low-saturation colors for comfort with refined accents for highlights.
pub mod colors {
    use ftui::render::cell::PackedRgba as Color;

    // ═══════════════════════════════════════════════════════════════════════════
    // BASE COLORS - The foundation of the UI
    // ═══════════════════════════════════════════════════════════════════════════

    /// Deep background - primary canvas color
    pub const BG_DEEP: Color = Color::rgb(26, 27, 38); // #1a1b26

    /// Elevated surface - cards, modals, popups
    pub const BG_SURFACE: Color = Color::rgb(36, 40, 59); // #24283b

    /// Subtle surface - hover states, selected items
    pub const BG_HIGHLIGHT: Color = Color::rgb(41, 46, 66); // #292e42

    /// Border color - subtle separators
    pub const BORDER: Color = Color::rgb(59, 66, 97); // #3b4261

    /// Border accent - focused/active elements
    pub const BORDER_FOCUS: Color = Color::rgb(125, 145, 200); // #7d91c8

    // ═══════════════════════════════════════════════════════════════════════════
    // TEXT COLORS - Hierarchical text styling
    // ═══════════════════════════════════════════════════════════════════════════

    /// Primary text - headings, important content
    pub const TEXT_PRIMARY: Color = Color::rgb(192, 202, 245); // #c0caf5

    /// Secondary text - body content
    pub const TEXT_SECONDARY: Color = Color::rgb(169, 177, 214); // #a9b1d6

    /// Muted text - hints, placeholders, timestamps
    /// Lightened from original Tokyo Night #565f89 to meet WCAG AA-large (3:1) contrast
    pub const TEXT_MUTED: Color = Color::rgb(105, 114, 158); // #696e9e (WCAG AA-large compliant)

    /// Disabled/inactive text
    pub const TEXT_DISABLED: Color = Color::rgb(68, 75, 106); // #444b6a

    // ═══════════════════════════════════════════════════════════════════════════
    // ACCENT COLORS - Brand and interaction highlights
    // ═══════════════════════════════════════════════════════════════════════════

    /// Primary accent - main actions, links, focus states
    pub const ACCENT_PRIMARY: Color = Color::rgb(122, 162, 247); // #7aa2f7

    /// Secondary accent - complementary highlights
    pub const ACCENT_SECONDARY: Color = Color::rgb(187, 154, 247); // #bb9af7

    /// Tertiary accent - subtle highlights
    pub const ACCENT_TERTIARY: Color = Color::rgb(125, 207, 255); // #7dcfff

    // ═══════════════════════════════════════════════════════════════════════════
    // SEMANTIC COLORS - Role-based coloring (muted versions)
    // ═══════════════════════════════════════════════════════════════════════════

    /// User messages - soft sage green
    pub const ROLE_USER: Color = Color::rgb(158, 206, 106); // #9ece6a

    /// Agent/Assistant messages - matches primary accent
    pub const ROLE_AGENT: Color = Color::rgb(122, 162, 247); // #7aa2f7

    /// Tool invocations - warm peach
    pub const ROLE_TOOL: Color = Color::rgb(255, 158, 100); // #ff9e64

    /// System messages - soft amber
    pub const ROLE_SYSTEM: Color = Color::rgb(224, 175, 104); // #e0af68

    // ═══════════════════════════════════════════════════════════════════════════
    // STATUS COLORS - Feedback and state indication
    // ═══════════════════════════════════════════════════════════════════════════

    /// Success states
    pub const STATUS_SUCCESS: Color = Color::rgb(115, 218, 202); // #73daca

    /// Warning states
    pub const STATUS_WARNING: Color = Color::rgb(224, 175, 104); // #e0af68

    /// Error states
    pub const STATUS_ERROR: Color = Color::rgb(247, 118, 142); // #f7768e

    /// Info states
    pub const STATUS_INFO: Color = Color::rgb(125, 207, 255); // #7dcfff

    // ═══════════════════════════════════════════════════════════════════════════
    // AGENT-SPECIFIC TINTS - Distinct background variations per agent
    // ═══════════════════════════════════════════════════════════════════════════

    /// Claude Code - distinct blue tint
    pub const AGENT_CLAUDE_BG: Color = Color::rgb(24, 30, 52); // #181e34 - blue

    /// Codex - distinct green tint
    pub const AGENT_CODEX_BG: Color = Color::rgb(22, 38, 32); // #162620 - green

    /// Cline - distinct cyan tint
    pub const AGENT_CLINE_BG: Color = Color::rgb(20, 34, 42); // #14222a - cyan

    /// Gemini - distinct purple tint
    pub const AGENT_GEMINI_BG: Color = Color::rgb(34, 24, 48); // #221830 - purple

    /// Amp - distinct warm/orange tint
    pub const AGENT_AMP_BG: Color = Color::rgb(42, 28, 24); // #2a1c18 - warm

    /// Aider - distinct teal tint
    pub const AGENT_AIDER_BG: Color = Color::rgb(20, 36, 36); // #142424 - teal

    /// Cursor - distinct magenta tint
    pub const AGENT_CURSOR_BG: Color = Color::rgb(38, 24, 38); // #261826 - magenta

    /// ChatGPT - distinct emerald tint
    pub const AGENT_CHATGPT_BG: Color = Color::rgb(20, 38, 28); // #14261c - emerald

    /// `OpenCode` - neutral gray
    pub const AGENT_OPENCODE_BG: Color = Color::rgb(32, 32, 36); // #202024 - neutral

    // ═══════════════════════════════════════════════════════════════════════════
    // ROLE-AWARE BACKGROUND TINTS - Subtle backgrounds per message type
    // ═══════════════════════════════════════════════════════════════════════════

    /// User message background - subtle green tint
    pub const ROLE_USER_BG: Color = Color::rgb(26, 32, 30); // #1a201e

    /// Assistant/agent message background - subtle blue tint
    pub const ROLE_AGENT_BG: Color = Color::rgb(26, 28, 36); // #1a1c24

    /// Tool invocation background - subtle orange/warm tint
    pub const ROLE_TOOL_BG: Color = Color::rgb(32, 28, 26); // #201c1a

    /// System message background - subtle amber tint
    pub const ROLE_SYSTEM_BG: Color = Color::rgb(32, 30, 26); // #201e1a

    // ═══════════════════════════════════════════════════════════════════════════
    // GRADIENT SIMULATION COLORS - Multi-shade for depth effects
    // ═══════════════════════════════════════════════════════════════════════════

    /// Header gradient top - darkest shade
    pub const GRADIENT_HEADER_TOP: Color = Color::rgb(22, 24, 32); // #161820

    /// Header gradient middle - mid shade
    pub const GRADIENT_HEADER_MID: Color = Color::rgb(30, 32, 44); // #1e202c

    /// Header gradient bottom - lightest shade
    pub const GRADIENT_HEADER_BOT: Color = Color::rgb(36, 40, 54); // #242836

    /// Pill gradient left
    pub const GRADIENT_PILL_LEFT: Color = Color::rgb(50, 56, 80); // #323850

    /// Pill gradient center
    pub const GRADIENT_PILL_CENTER: Color = Color::rgb(60, 68, 96); // #3c4460

    /// Pill gradient right
    pub const GRADIENT_PILL_RIGHT: Color = Color::rgb(50, 56, 80); // #323850

    // ═══════════════════════════════════════════════════════════════════════════
    // BORDER VARIANTS - For adaptive width styling
    // ═══════════════════════════════════════════════════════════════════════════

    /// Subtle border - for narrow terminals
    pub const BORDER_MINIMAL: Color = Color::rgb(45, 50, 72); // #2d3248

    /// Standard border - normal terminals
    pub const BORDER_STANDARD: Color = Color::rgb(59, 66, 97); // #3b4261 (same as BORDER)

    /// Emphasized border - for wide terminals
    pub const BORDER_EMPHASIZED: Color = Color::rgb(75, 85, 120); // #4b5578
}

/// Complete styling for a message role (user, assistant, tool, system).
#[derive(Clone, Copy)]
pub struct RoleTheme {
    /// Foreground (text) color
    pub fg: PackedRgba,
    /// Background tint (subtle)
    pub bg: PackedRgba,
    /// Border/accent color
    pub border: PackedRgba,
    /// Badge/indicator color
    pub badge: PackedRgba,
}

/// Gradient shades for simulating depth effects in headers/pills.
#[derive(Clone, Copy)]
pub struct GradientShades {
    /// Darkest shade (top/edges)
    pub dark: PackedRgba,
    /// Mid-tone shade
    pub mid: PackedRgba,
    /// Lightest shade (center/bottom)
    pub light: PackedRgba,
}

impl GradientShades {
    /// Header gradient - darkest at top, lightest at bottom
    pub fn header() -> Self {
        Self {
            dark: colors::GRADIENT_HEADER_TOP,
            mid: colors::GRADIENT_HEADER_MID,
            light: colors::GRADIENT_HEADER_BOT,
        }
    }

    /// Pill gradient - darker at edges, lighter in center
    pub fn pill() -> Self {
        Self {
            dark: colors::GRADIENT_PILL_LEFT,
            mid: colors::GRADIENT_PILL_CENTER,
            light: colors::GRADIENT_PILL_RIGHT,
        }
    }

    /// Create styles for each shade
    pub fn styles(&self) -> (Style, Style, Style) {
        (
            Style::new().bg(self.dark),
            Style::new().bg(self.mid),
            Style::new().bg(self.light),
        )
    }
}

/// Terminal width classification for adaptive styling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalWidth {
    /// Narrow terminal (<80 cols) - minimal decorations
    Narrow,
    /// Normal terminal (80-120 cols) - standard styling
    Normal,
    /// Wide terminal (>120 cols) - enhanced decorations
    Wide,
}

impl TerminalWidth {
    /// Classify terminal width from column count
    pub fn from_cols(cols: u16) -> Self {
        if cols < 80 {
            Self::Narrow
        } else if cols <= 120 {
            Self::Normal
        } else {
            Self::Wide
        }
    }

    /// Get the appropriate border color for this width
    pub fn border_color(self) -> PackedRgba {
        match self {
            Self::Narrow => colors::BORDER_MINIMAL,
            Self::Normal => colors::BORDER_STANDARD,
            Self::Wide => colors::BORDER_EMPHASIZED,
        }
    }

    /// Get border style for this width
    pub fn border_style(self) -> Style {
        Style::new().fg(self.border_color())
    }

    /// Should we show decorative elements at this width?
    pub fn show_decorations(self) -> bool {
        !matches!(self, Self::Narrow)
    }

    /// Should we show extended info panels at this width?
    pub fn show_extended_info(self) -> bool {
        matches!(self, Self::Wide)
    }
}

/// Adaptive border configuration based on terminal width.
#[derive(Clone, Copy)]
pub struct AdaptiveBorders {
    /// Current terminal width classification
    pub width_class: TerminalWidth,
    /// Border color
    pub color: PackedRgba,
    /// Border style
    pub style: Style,
    /// Use double borders for emphasis
    pub use_double: bool,
    /// Show corner decorations
    pub show_corners: bool,
}

impl AdaptiveBorders {
    /// Create adaptive borders for the given terminal width
    pub fn for_width(cols: u16) -> Self {
        let width_class = TerminalWidth::from_cols(cols);
        let color = width_class.border_color();
        Self {
            width_class,
            color,
            style: Style::new().fg(color),
            use_double: matches!(width_class, TerminalWidth::Wide),
            show_corners: width_class.show_decorations(),
        }
    }

    /// Create borders for focused/active elements
    pub fn focused(cols: u16) -> Self {
        let mut borders = Self::for_width(cols);
        borders.color = colors::BORDER_FOCUS;
        borders.style = Style::new().fg(colors::BORDER_FOCUS);
        borders
    }
}

#[derive(Clone, Copy)]
pub struct PaneTheme {
    pub bg: PackedRgba,
    pub fg: PackedRgba,
    pub accent: PackedRgba,
}

#[derive(Clone, Copy)]
pub struct ThemePalette {
    pub accent: PackedRgba,
    pub accent_alt: PackedRgba,
    pub bg: PackedRgba,
    pub fg: PackedRgba,
    pub surface: PackedRgba,
    pub hint: PackedRgba,
    pub border: PackedRgba,
    pub user: PackedRgba,
    pub agent: PackedRgba,
    pub tool: PackedRgba,
    pub system: PackedRgba,
    /// Alternating stripe colors for zebra-striping results (sux.6.3)
    pub stripe_even: PackedRgba,
    pub stripe_odd: PackedRgba,
}

impl ThemePalette {
    /// Light theme - clean, minimal, professional
    pub fn light() -> Self {
        Self {
            accent: PackedRgba::rgb(47, 107, 231),       // Rich blue
            accent_alt: PackedRgba::rgb(124, 93, 198),   // Purple
            bg: PackedRgba::rgb(250, 250, 252),          // Off-white
            fg: PackedRgba::rgb(36, 41, 46),             // Near-black
            surface: PackedRgba::rgb(240, 241, 245),     // Light gray
            hint: PackedRgba::rgb(125, 134, 144),        // Medium gray (higher contrast)
            border: PackedRgba::rgb(216, 222, 228),      // Border gray
            user: PackedRgba::rgb(45, 138, 72),          // Forest green
            agent: PackedRgba::rgb(47, 107, 231),        // Rich blue
            tool: PackedRgba::rgb(207, 107, 44),         // Warm orange
            system: PackedRgba::rgb(177, 133, 41),       // Amber
            stripe_even: PackedRgba::rgb(250, 250, 252), // Same as bg
            stripe_odd: PackedRgba::rgb(240, 241, 245),  // Slightly darker
        }
    }

    /// Dark theme - premium, refined, easy on the eyes
    pub fn dark() -> Self {
        Self {
            accent: colors::ACCENT_PRIMARY,
            accent_alt: colors::ACCENT_SECONDARY,
            bg: colors::BG_DEEP,
            fg: colors::TEXT_PRIMARY,
            surface: colors::BG_SURFACE,
            hint: colors::TEXT_MUTED,
            border: colors::BORDER,
            user: colors::ROLE_USER,
            agent: colors::ROLE_AGENT,
            tool: colors::ROLE_TOOL,
            system: colors::ROLE_SYSTEM,
            stripe_even: colors::BG_DEEP,       // #1a1b26
            stripe_odd: PackedRgba::rgb(30, 32, 48), // #1e2030 - slightly lighter
        }
    }

    /// Title style - accent colored with bold modifier
    pub fn title(self) -> Style {
        Style::new()
            .fg(self.accent)
            .bold()
    }

    /// Subtle title style - less prominent headers
    pub fn title_subtle(self) -> Style {
        Style::new().fg(self.fg).bold()
    }

    /// Hint text style - for secondary/muted information
    pub fn hint_style(self) -> Style {
        Style::new().fg(self.hint)
    }

    /// Border style - for unfocused elements
    pub fn border_style(self) -> Style {
        Style::new().fg(self.border)
    }

    /// Focused border style - for active elements
    pub fn border_focus_style(self) -> Style {
        Style::new().fg(colors::BORDER_FOCUS)
    }

    /// Surface style - for cards, modals, elevated content
    pub fn surface_style(self) -> Style {
        Style::new().bg(self.surface)
    }

    /// Per-agent pane colors - distinct tinted backgrounds with consistent text colors.
    ///
    /// Design philosophy: Each agent gets a visually distinct background color that makes
    /// it immediately clear which tool produced the result. Accent colors are chosen to
    /// complement the background while remaining cohesive.
    pub fn agent_pane(agent: &str) -> PaneTheme {
        let slug = agent.to_lowercase().replace('-', "_");

        let (bg, accent) = match slug.as_str() {
            // Core agents with distinct color identities
            "claude_code" | "claude" => (colors::AGENT_CLAUDE_BG, colors::ACCENT_PRIMARY), // Blue
            "codex" => (colors::AGENT_CODEX_BG, colors::STATUS_SUCCESS),                   // Green
            "cline" => (colors::AGENT_CLINE_BG, colors::ACCENT_TERTIARY),                  // Cyan
            "gemini" | "gemini_cli" => (colors::AGENT_GEMINI_BG, colors::ACCENT_SECONDARY), // Purple
            "amp" => (colors::AGENT_AMP_BG, colors::STATUS_ERROR), // Orange/Red
            "aider" => (colors::AGENT_AIDER_BG, PackedRgba::rgb(64, 224, 208)), // Turquoise accent
            "cursor" => (colors::AGENT_CURSOR_BG, PackedRgba::rgb(236, 72, 153)), // Pink accent
            "chatgpt" => (colors::AGENT_CHATGPT_BG, PackedRgba::rgb(16, 163, 127)), // ChatGPT green
            "opencode" => (colors::AGENT_OPENCODE_BG, colors::ROLE_USER), // Neutral/sage
            "pi_agent" => (colors::AGENT_CODEX_BG, PackedRgba::rgb(255, 140, 0)), // Orange for pi
            _ => (colors::BG_DEEP, colors::ACCENT_PRIMARY),
        };

        PaneTheme {
            bg,
            fg: colors::TEXT_PRIMARY, // Consistent, legible text
            accent,
        }
    }

    /// Returns a small, legible icon for the given agent slug.
    /// Icons favor single-width glyphs to avoid layout jitter in result headers.
    pub fn agent_icon(agent: &str) -> &'static str {
        match agent.to_lowercase().as_str() {
            "codex" => "🔹",
            "claude_code" | "claude" => "🤖",
            "gemini" | "gemini_cli" => "💎",
            "cline" => "🧭",
            "amp" => "⚡",
            "aider" => "🔧",
            "cursor" => "🎯",
            "chatgpt" => "💬",
            "opencode" => "📦",
            "pi_agent" => "🥧",
            _ => "✨",
        }
    }

    /// Get a role-specific style for message rendering
    pub fn role_style(self, role: &str) -> Style {
        let color = match role.to_lowercase().as_str() {
            "user" => self.user,
            "assistant" | "agent" => self.agent,
            "tool" => self.tool,
            "system" => self.system,
            _ => self.hint,
        };
        Style::new().fg(color)
    }

    /// Get a complete `RoleTheme` for a message role with full styling options.
    ///
    /// Includes foreground, background tint, border, and badge colors for
    /// comprehensive role-aware message rendering.
    pub fn role_theme(self, role: &str) -> RoleTheme {
        match role.to_lowercase().as_str() {
            "user" => RoleTheme {
                fg: self.user,
                bg: colors::ROLE_USER_BG,
                border: self.user,
                badge: colors::STATUS_SUCCESS,
            },
            "assistant" | "agent" => RoleTheme {
                fg: self.agent,
                bg: colors::ROLE_AGENT_BG,
                border: self.agent,
                badge: colors::ACCENT_PRIMARY,
            },
            "tool" => RoleTheme {
                fg: self.tool,
                bg: colors::ROLE_TOOL_BG,
                border: self.tool,
                badge: colors::ROLE_TOOL,
            },
            "system" => RoleTheme {
                fg: self.system,
                bg: colors::ROLE_SYSTEM_BG,
                border: self.system,
                badge: colors::STATUS_WARNING,
            },
            _ => RoleTheme {
                fg: self.hint,
                bg: self.bg,
                border: self.border,
                badge: self.hint,
            },
        }
    }

    /// Get the gradient shades for header backgrounds
    pub fn header_gradient(&self) -> GradientShades {
        GradientShades::header()
    }

    /// Get the gradient shades for pills/badges
    pub fn pill_gradient(&self) -> GradientShades {
        GradientShades::pill()
    }

    /// Get adaptive borders for the given terminal width
    pub fn adaptive_borders(&self, cols: u16) -> AdaptiveBorders {
        AdaptiveBorders::for_width(cols)
    }

    /// Get focused adaptive borders for the given terminal width
    pub fn adaptive_borders_focused(&self, cols: u16) -> AdaptiveBorders {
        AdaptiveBorders::focused(cols)
    }

    /// Highlighted text style - for search matches
    /// Uses high-contrast background with theme-aware foreground for visibility
    pub fn highlight_style(self) -> Style {
        Style::new()
            .fg(self.bg) // Dark text on light bg, light text on dark bg
            .bg(self.accent) // Accent color background for high visibility
            .bold()
    }

    /// Selected item style - for list selections
    pub fn selected_style(self) -> Style {
        Style::new()
            .bg(colors::BG_HIGHLIGHT)
            .bold()
    }

    /// Code block background style
    pub fn code_style(self) -> Style {
        Style::new()
            .bg(colors::BG_SURFACE)
            .fg(colors::TEXT_SECONDARY)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STYLE HELPERS - Common style patterns
// ═══════════════════════════════════════════════════════════════════════════════

/// Creates a subtle badge/chip style for filter indicators
pub fn chip_style(palette: ThemePalette) -> Style {
    Style::new()
        .fg(palette.accent_alt)
        .bold()
}

/// Creates a keyboard shortcut style (for help text)
pub fn kbd_style(palette: ThemePalette) -> Style {
    Style::new()
        .fg(palette.accent)
        .bold()
}

/// Creates style for score indicators based on magnitude
pub fn score_style(score: f32, palette: ThemePalette) -> Style {
    let color = if score >= 8.0 {
        colors::STATUS_SUCCESS
    } else if score >= 5.0 {
        palette.accent
    } else {
        palette.hint
    };

    let base = Style::new().fg(color);
    if score >= 8.0 {
        base.bold()
    } else if score < 5.0 {
        base.dim()
    } else {
        base
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONTRAST UTILITIES - WCAG compliance helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Calculate relative luminance of an RGB color per WCAG 2.1.
/// Returns a value from 0.0 (black) to 1.0 (white).
pub fn relative_luminance(color: PackedRgba) -> f64 {
    let (r, g, b) = (color.r(), color.g(), color.b());

    fn linearize(c: u8) -> f64 {
        let c = f64::from(c) / 255.0;
        if c <= 0.04045 {
            c / 12.92
        } else {
            ((c + 0.055) / 1.055).powf(2.4)
        }
    }

    let r_lin = linearize(r);
    let g_lin = linearize(g);
    let b_lin = linearize(b);

    0.2126 * r_lin + 0.7152 * g_lin + 0.0722 * b_lin
}

/// Calculate WCAG contrast ratio between two colors.
/// Returns a value from 1.0 (no contrast) to 21.0 (black/white).
pub fn contrast_ratio(fg: PackedRgba, bg: PackedRgba) -> f64 {
    let lum_fg = relative_luminance(fg);
    let lum_bg = relative_luminance(bg);
    let (lighter, darker) = if lum_fg > lum_bg {
        (lum_fg, lum_bg)
    } else {
        (lum_bg, lum_fg)
    };
    (lighter + 0.05) / (darker + 0.05)
}

/// WCAG compliance level for contrast ratios.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContrastLevel {
    /// Fails WCAG requirements (ratio < 3.0)
    Fail,
    /// WCAG AA for large text (ratio >= 3.0)
    AALarge,
    /// WCAG AA for normal text (ratio >= 4.5)
    AA,
    /// WCAG AAA for large text (ratio >= 4.5)
    AAALarge,
    /// WCAG AAA for normal text (ratio >= 7.0)
    AAA,
}

impl ContrastLevel {
    /// Determine WCAG compliance level from a contrast ratio
    pub fn from_ratio(ratio: f64) -> Self {
        if ratio >= 7.0 {
            Self::AAA
        } else if ratio >= 4.5 {
            Self::AA
        } else if ratio >= 3.0 {
            Self::AALarge
        } else {
            Self::Fail
        }
    }

    /// Check if this level meets the specified minimum requirement
    pub fn meets(self, required: ContrastLevel) -> bool {
        match required {
            Self::Fail => true,
            Self::AALarge => !matches!(self, Self::Fail),
            Self::AA | Self::AAALarge => matches!(self, Self::AA | Self::AAALarge | Self::AAA),
            Self::AAA => matches!(self, Self::AAA),
        }
    }

    /// Display name for this compliance level
    pub fn name(self) -> &'static str {
        match self {
            Self::Fail => "Fail",
            Self::AALarge => "AA (large text)",
            Self::AA => "AA",
            Self::AAALarge => "AAA (large text)",
            Self::AAA => "AAA",
        }
    }
}

/// Check contrast compliance between foreground and background colors.
pub fn check_contrast(fg: PackedRgba, bg: PackedRgba) -> ContrastLevel {
    ContrastLevel::from_ratio(contrast_ratio(fg, bg))
}

/// Ensure a color meets minimum contrast against a background.
/// If the color doesn't meet the requirement, returns a suggested alternative.
pub fn ensure_contrast(fg: PackedRgba, bg: PackedRgba, min_level: ContrastLevel) -> PackedRgba {
    let level = check_contrast(fg, bg);
    if level.meets(min_level) {
        return fg;
    }

    // Try lightening or darkening the foreground
    let bg_lum = relative_luminance(bg);
    if bg_lum > 0.5 {
        // Light background, use black for maximum contrast
        PackedRgba::BLACK
    } else {
        // Dark background, use white for maximum contrast
        PackedRgba::WHITE
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// THEME PRESETS - Popular color schemes for user preference
// ═══════════════════════════════════════════════════════════════════════════════

/// Available theme presets that users can cycle through.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThemePreset {
    /// Default dark theme - Tokyo Night inspired, premium feel
    #[default]
    Dark,
    /// Light theme - clean, minimal, professional
    Light,
    /// Catppuccin Mocha - warm, pastel colors
    Catppuccin,
    /// Dracula - purple-tinted dark theme
    Dracula,
    /// Nord - arctic, cool blue tones
    Nord,
    /// High Contrast - maximum contrast for accessibility (WCAG AAA)
    HighContrast,
}

impl ThemePreset {
    /// Get the display name for this theme preset
    pub fn name(self) -> &'static str {
        match self {
            Self::Dark => "Dark",
            Self::Light => "Light",
            Self::Catppuccin => "Catppuccin",
            Self::Dracula => "Dracula",
            Self::Nord => "Nord",
            Self::HighContrast => "High Contrast",
        }
    }

    /// Cycle to the next theme preset
    pub fn next(self) -> Self {
        match self {
            Self::Dark => Self::Light,
            Self::Light => Self::Catppuccin,
            Self::Catppuccin => Self::Dracula,
            Self::Dracula => Self::Nord,
            Self::Nord => Self::HighContrast,
            Self::HighContrast => Self::Dark,
        }
    }

    /// Cycle to the previous theme preset
    pub fn prev(self) -> Self {
        match self {
            Self::Dark => Self::HighContrast,
            Self::Light => Self::Dark,
            Self::Catppuccin => Self::Light,
            Self::Dracula => Self::Catppuccin,
            Self::Nord => Self::Dracula,
            Self::HighContrast => Self::Nord,
        }
    }

    /// Convert this preset to its `ThemePalette`
    pub fn to_palette(self) -> ThemePalette {
        match self {
            Self::Dark => ThemePalette::dark(),
            Self::Light => ThemePalette::light(),
            Self::Catppuccin => ThemePalette::catppuccin(),
            Self::Dracula => ThemePalette::dracula(),
            Self::Nord => ThemePalette::nord(),
            Self::HighContrast => ThemePalette::high_contrast(),
        }
    }

    /// List all available presets
    pub fn all() -> &'static [Self] {
        &[
            Self::Dark,
            Self::Light,
            Self::Catppuccin,
            Self::Dracula,
            Self::Nord,
            Self::HighContrast,
        ]
    }
}

impl ThemePalette {
    /// Catppuccin Mocha theme - warm, pastel colors
    /// <https://github.com/catppuccin/catppuccin>
    pub fn catppuccin() -> Self {
        Self {
            // Catppuccin Mocha palette
            accent: PackedRgba::rgb(137, 180, 250),     // Blue
            accent_alt: PackedRgba::rgb(203, 166, 247), // Mauve
            bg: PackedRgba::rgb(30, 30, 46),            // Base
            fg: PackedRgba::rgb(205, 214, 244),         // Text
            surface: PackedRgba::rgb(49, 50, 68),       // Surface0
            hint: PackedRgba::rgb(127, 132, 156),       // Overlay1
            border: PackedRgba::rgb(69, 71, 90),        // Surface1
            user: PackedRgba::rgb(166, 227, 161),       // Green
            agent: PackedRgba::rgb(137, 180, 250),      // Blue
            tool: PackedRgba::rgb(250, 179, 135),       // Peach
            system: PackedRgba::rgb(249, 226, 175),     // Yellow
            stripe_even: PackedRgba::rgb(30, 30, 46),   // Base
            stripe_odd: PackedRgba::rgb(36, 36, 54),    // Slightly lighter
        }
    }

    /// Dracula theme - purple-tinted dark theme
    /// <https://draculatheme.com>/
    pub fn dracula() -> Self {
        Self {
            // Dracula palette
            accent: PackedRgba::rgb(189, 147, 249),     // Purple
            accent_alt: PackedRgba::rgb(255, 121, 198), // Pink
            bg: PackedRgba::rgb(40, 42, 54),            // Background
            fg: PackedRgba::rgb(248, 248, 242),         // Foreground
            surface: PackedRgba::rgb(68, 71, 90),       // Current Line
            hint: PackedRgba::rgb(155, 165, 200), // Lightened from Dracula comment for WCAG AA-large on surface
            border: PackedRgba::rgb(68, 71, 90),  // Current Line
            user: PackedRgba::rgb(80, 250, 123),  // Green
            agent: PackedRgba::rgb(189, 147, 249), // Purple
            tool: PackedRgba::rgb(255, 184, 108), // Orange
            system: PackedRgba::rgb(241, 250, 140), // Yellow
            stripe_even: PackedRgba::rgb(40, 42, 54), // Background
            stripe_odd: PackedRgba::rgb(48, 50, 64), // Slightly lighter
        }
    }

    /// Nord theme - arctic, cool blue tones
    /// <https://www.nordtheme.com>/
    pub fn nord() -> Self {
        Self {
            // Nord palette
            accent: PackedRgba::rgb(136, 192, 208), // Nord8 (frost cyan)
            accent_alt: PackedRgba::rgb(180, 142, 173), // Nord15 (aurora purple)
            bg: PackedRgba::rgb(46, 52, 64),        // Nord0 (polar night)
            fg: PackedRgba::rgb(236, 239, 244),     // Nord6 (snow storm)
            surface: PackedRgba::rgb(59, 66, 82),   // Nord1
            hint: PackedRgba::rgb(145, 155, 180),   // Lightened from Nord3 for WCAG AA-large on surface
            border: PackedRgba::rgb(67, 76, 94),    // Nord2
            user: PackedRgba::rgb(163, 190, 140),   // Nord14 (aurora green)
            agent: PackedRgba::rgb(136, 192, 208),  // Nord8 (frost cyan)
            tool: PackedRgba::rgb(208, 135, 112),   // Nord12 (aurora orange)
            system: PackedRgba::rgb(235, 203, 139), // Nord13 (aurora yellow)
            stripe_even: PackedRgba::rgb(46, 52, 64), // Nord0
            stripe_odd: PackedRgba::rgb(52, 58, 72), // Slightly lighter
        }
    }

    /// High Contrast theme - maximum contrast for accessibility
    ///
    /// Designed to meet WCAG AAA standards (7:1 contrast ratio).
    /// Uses pure black/white with saturated accent colors for maximum visibility.
    pub fn high_contrast() -> Self {
        Self {
            // High contrast palette - pure black background, white text
            accent: PackedRgba::rgb(0, 191, 255), // Bright cyan - high visibility
            accent_alt: PackedRgba::rgb(255, 105, 180), // Hot pink - distinct secondary
            bg: PackedRgba::BLACK,                 // Pure black
            fg: PackedRgba::WHITE,                 // Pure white
            surface: PackedRgba::rgb(28, 28, 28), // Very dark gray for elevation
            hint: PackedRgba::rgb(180, 180, 180), // Light gray - still readable
            border: PackedRgba::WHITE,             // White borders for clear separation
            user: PackedRgba::rgb(0, 255, 127),   // Bright spring green
            agent: PackedRgba::rgb(0, 191, 255),  // Bright cyan (matches accent)
            tool: PackedRgba::rgb(255, 165, 0),   // Bright orange
            system: PackedRgba::rgb(255, 255, 0), // Pure yellow
            stripe_even: PackedRgba::BLACK,        // Pure black
            stripe_odd: PackedRgba::rgb(24, 24, 24), // Very dark gray
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== TerminalWidth tests ====================

    #[test]
    fn test_terminal_width_from_cols_narrow() {
        assert_eq!(TerminalWidth::from_cols(40), TerminalWidth::Narrow);
        assert_eq!(TerminalWidth::from_cols(79), TerminalWidth::Narrow);
    }

    #[test]
    fn test_terminal_width_from_cols_normal() {
        assert_eq!(TerminalWidth::from_cols(80), TerminalWidth::Normal);
        assert_eq!(TerminalWidth::from_cols(100), TerminalWidth::Normal);
        assert_eq!(TerminalWidth::from_cols(120), TerminalWidth::Normal);
    }

    #[test]
    fn test_terminal_width_from_cols_wide() {
        assert_eq!(TerminalWidth::from_cols(121), TerminalWidth::Wide);
        assert_eq!(TerminalWidth::from_cols(200), TerminalWidth::Wide);
    }

    #[test]
    fn test_terminal_width_border_color() {
        assert_eq!(TerminalWidth::Narrow.border_color(), colors::BORDER_MINIMAL);
        assert_eq!(
            TerminalWidth::Normal.border_color(),
            colors::BORDER_STANDARD
        );
        assert_eq!(
            TerminalWidth::Wide.border_color(),
            colors::BORDER_EMPHASIZED
        );
    }

    #[test]
    fn test_terminal_width_show_decorations() {
        assert!(!TerminalWidth::Narrow.show_decorations());
        assert!(TerminalWidth::Normal.show_decorations());
        assert!(TerminalWidth::Wide.show_decorations());
    }

    #[test]
    fn test_terminal_width_show_extended_info() {
        assert!(!TerminalWidth::Narrow.show_extended_info());
        assert!(!TerminalWidth::Normal.show_extended_info());
        assert!(TerminalWidth::Wide.show_extended_info());
    }

    // ==================== GradientShades tests ====================

    #[test]
    fn test_gradient_shades_header() {
        let shades = GradientShades::header();
        assert_eq!(shades.dark, colors::GRADIENT_HEADER_TOP);
        assert_eq!(shades.mid, colors::GRADIENT_HEADER_MID);
        assert_eq!(shades.light, colors::GRADIENT_HEADER_BOT);
    }

    #[test]
    fn test_gradient_shades_pill() {
        let shades = GradientShades::pill();
        assert_eq!(shades.dark, colors::GRADIENT_PILL_LEFT);
        assert_eq!(shades.mid, colors::GRADIENT_PILL_CENTER);
        assert_eq!(shades.light, colors::GRADIENT_PILL_RIGHT);
    }

    #[test]
    fn test_gradient_shades_styles() {
        let shades = GradientShades::header();
        let (dark, mid, light) = shades.styles();
        assert_eq!(dark.bg, Some(shades.dark));
        assert_eq!(mid.bg, Some(shades.mid));
        assert_eq!(light.bg, Some(shades.light));
    }

    // ==================== AdaptiveBorders tests ====================

    #[test]
    fn test_adaptive_borders_for_width_narrow() {
        let borders = AdaptiveBorders::for_width(60);
        assert_eq!(borders.width_class, TerminalWidth::Narrow);
        assert!(!borders.use_double);
        assert!(!borders.show_corners);
    }

    #[test]
    fn test_adaptive_borders_for_width_normal() {
        let borders = AdaptiveBorders::for_width(100);
        assert_eq!(borders.width_class, TerminalWidth::Normal);
        assert!(!borders.use_double);
        assert!(borders.show_corners);
    }

    #[test]
    fn test_adaptive_borders_for_width_wide() {
        let borders = AdaptiveBorders::for_width(150);
        assert_eq!(borders.width_class, TerminalWidth::Wide);
        assert!(borders.use_double);
        assert!(borders.show_corners);
    }

    #[test]
    fn test_adaptive_borders_focused() {
        let borders = AdaptiveBorders::focused(100);
        assert_eq!(borders.color, colors::BORDER_FOCUS);
    }

    // ==================== ThemePalette tests ====================

    #[test]
    fn test_theme_palette_light() {
        let palette = ThemePalette::light();
        // Light theme should have a light background
        assert_eq!(palette.bg, PackedRgba::rgb(250, 250, 252));
        // And dark foreground
        assert_eq!(palette.fg, PackedRgba::rgb(36, 41, 46));
    }

    #[test]
    fn test_theme_palette_dark() {
        let palette = ThemePalette::dark();
        // Dark theme should have a dark background
        assert_eq!(palette.bg, colors::BG_DEEP);
        // And light foreground
        assert_eq!(palette.fg, colors::TEXT_PRIMARY);
    }

    #[test]
    fn test_theme_palette_catppuccin() {
        let palette = ThemePalette::catppuccin();
        // Check specific Catppuccin colors
        assert_eq!(palette.bg, PackedRgba::rgb(30, 30, 46));
    }

    #[test]
    fn test_theme_palette_dracula() {
        let palette = ThemePalette::dracula();
        assert_eq!(palette.bg, PackedRgba::rgb(40, 42, 54));
    }

    #[test]
    fn test_theme_palette_nord() {
        let palette = ThemePalette::nord();
        assert_eq!(palette.bg, PackedRgba::rgb(46, 52, 64));
    }

    #[test]
    fn test_theme_palette_high_contrast() {
        let palette = ThemePalette::high_contrast();
        // High contrast should use pure black and white
        assert_eq!(palette.bg, PackedRgba::rgb(0, 0, 0));
        assert_eq!(palette.fg, PackedRgba::rgb(255, 255, 255));
    }

    #[test]
    fn test_theme_palette_agent_pane_known_agents() {
        // Test known agent color mappings
        let claude = ThemePalette::agent_pane("claude_code");
        assert_eq!(claude.bg, colors::AGENT_CLAUDE_BG);

        let codex = ThemePalette::agent_pane("codex");
        assert_eq!(codex.bg, colors::AGENT_CODEX_BG);

        let gemini = ThemePalette::agent_pane("gemini_cli");
        assert_eq!(gemini.bg, colors::AGENT_GEMINI_BG);

        let chatgpt = ThemePalette::agent_pane("chatgpt");
        assert_eq!(chatgpt.bg, colors::AGENT_CHATGPT_BG);
    }

    #[test]
    fn test_theme_palette_agent_pane_unknown_agent() {
        let unknown = ThemePalette::agent_pane("unknown_agent");
        assert_eq!(unknown.bg, colors::BG_DEEP);
    }

    #[test]
    fn test_theme_palette_agent_icon() {
        assert_eq!(ThemePalette::agent_icon("codex"), "🔹");
        assert_eq!(ThemePalette::agent_icon("claude_code"), "🤖");
        assert_eq!(ThemePalette::agent_icon("gemini"), "💎");
        assert_eq!(ThemePalette::agent_icon("chatgpt"), "💬");
        assert_eq!(ThemePalette::agent_icon("unknown"), "✨");
    }

    #[test]
    fn test_theme_palette_role_theme() {
        let palette = ThemePalette::dark();

        let user_theme = palette.role_theme("user");
        assert_eq!(user_theme.fg, palette.user);

        let agent_theme = palette.role_theme("assistant");
        assert_eq!(agent_theme.fg, palette.agent);

        let tool_theme = palette.role_theme("tool");
        assert_eq!(tool_theme.fg, palette.tool);

        let system_theme = palette.role_theme("system");
        assert_eq!(system_theme.fg, palette.system);
    }

    // ==================== ContrastLevel tests ====================

    #[test]
    fn test_contrast_level_from_ratio() {
        assert_eq!(ContrastLevel::from_ratio(2.0), ContrastLevel::Fail);
        assert_eq!(ContrastLevel::from_ratio(3.5), ContrastLevel::AALarge);
        assert_eq!(ContrastLevel::from_ratio(5.0), ContrastLevel::AA);
        assert_eq!(ContrastLevel::from_ratio(8.0), ContrastLevel::AAA);
    }

    #[test]
    fn test_contrast_level_meets() {
        assert!(ContrastLevel::AAA.meets(ContrastLevel::AA));
        assert!(ContrastLevel::AA.meets(ContrastLevel::AALarge));
        assert!(!ContrastLevel::Fail.meets(ContrastLevel::AA));
    }

    #[test]
    fn test_contrast_level_name() {
        assert_eq!(ContrastLevel::AAA.name(), "AAA");
        assert_eq!(ContrastLevel::AA.name(), "AA");
        assert_eq!(ContrastLevel::Fail.name(), "Fail");
    }

    // ==================== Luminance/Contrast tests ====================

    #[test]
    fn test_relative_luminance_black() {
        let lum = relative_luminance(PackedRgba::rgb(0, 0, 0));
        assert!((lum - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_relative_luminance_white() {
        let lum = relative_luminance(PackedRgba::rgb(255, 255, 255));
        assert!((lum - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_relative_luminance_named_colors() {
        // Black should have low luminance
        let black_lum = relative_luminance(PackedRgba::BLACK);
        assert!(black_lum < 0.01);

        // White should have high luminance
        let white_lum = relative_luminance(PackedRgba::WHITE);
        assert!(white_lum > 0.99);
    }

    #[test]
    fn test_contrast_ratio_black_white() {
        let ratio = contrast_ratio(PackedRgba::rgb(255, 255, 255), PackedRgba::rgb(0, 0, 0));
        // Maximum contrast is 21:1
        assert!(ratio > 20.0);
    }

    #[test]
    fn test_contrast_ratio_same_color() {
        let ratio = contrast_ratio(PackedRgba::rgb(128, 128, 128), PackedRgba::rgb(128, 128, 128));
        // Same color = 1:1 contrast
        assert!((ratio - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_check_contrast() {
        // High contrast pair
        let level = check_contrast(PackedRgba::rgb(255, 255, 255), PackedRgba::rgb(0, 0, 0));
        assert_eq!(level, ContrastLevel::AAA);

        // Low contrast pair (similar grays)
        let level = check_contrast(PackedRgba::rgb(100, 100, 100), PackedRgba::rgb(120, 120, 120));
        assert_eq!(level, ContrastLevel::Fail);
    }

    #[test]
    fn test_ensure_contrast_already_sufficient() {
        let bg = PackedRgba::rgb(0, 0, 0);
        let fg = PackedRgba::rgb(255, 255, 255);
        let result = ensure_contrast(fg, bg, ContrastLevel::AA);
        // Should return original since contrast is already good
        assert_eq!(result, fg);
    }

    // ==================== ThemePreset tests ====================

    #[test]
    fn test_theme_preset_default() {
        let preset = ThemePreset::default();
        assert_eq!(preset, ThemePreset::Dark);
    }

    #[test]
    fn test_theme_preset_name() {
        assert_eq!(ThemePreset::Dark.name(), "Dark");
        assert_eq!(ThemePreset::Light.name(), "Light");
        assert_eq!(ThemePreset::Catppuccin.name(), "Catppuccin");
        assert_eq!(ThemePreset::Dracula.name(), "Dracula");
        assert_eq!(ThemePreset::Nord.name(), "Nord");
        assert_eq!(ThemePreset::HighContrast.name(), "High Contrast");
    }

    #[test]
    fn test_theme_preset_next_cycles() {
        let mut preset = ThemePreset::Dark;
        preset = preset.next(); // Light
        assert_eq!(preset, ThemePreset::Light);
        preset = preset.next(); // Catppuccin
        assert_eq!(preset, ThemePreset::Catppuccin);
        preset = preset.next(); // Dracula
        assert_eq!(preset, ThemePreset::Dracula);
        preset = preset.next(); // Nord
        assert_eq!(preset, ThemePreset::Nord);
        preset = preset.next(); // HighContrast
        assert_eq!(preset, ThemePreset::HighContrast);
        preset = preset.next(); // Back to Dark
        assert_eq!(preset, ThemePreset::Dark);
    }

    #[test]
    fn test_theme_preset_prev_cycles() {
        let mut preset = ThemePreset::Dark;
        preset = preset.prev(); // HighContrast
        assert_eq!(preset, ThemePreset::HighContrast);
        preset = preset.prev(); // Nord
        assert_eq!(preset, ThemePreset::Nord);
    }

    #[test]
    fn test_theme_preset_to_palette() {
        let palette = ThemePreset::Dark.to_palette();
        assert_eq!(palette.bg, ThemePalette::dark().bg);

        let palette = ThemePreset::Light.to_palette();
        assert_eq!(palette.bg, ThemePalette::light().bg);
    }

    #[test]
    fn test_theme_preset_all() {
        let all = ThemePreset::all();
        assert_eq!(all.len(), 6);
        assert!(all.contains(&ThemePreset::Dark));
        assert!(all.contains(&ThemePreset::Light));
    }

    // ==================== Style helper tests ====================

    #[test]
    fn test_chip_style() {
        let palette = ThemePalette::dark();
        let style = chip_style(palette);
        assert_eq!(style.fg, Some(palette.accent_alt));
    }

    #[test]
    fn test_kbd_style() {
        let palette = ThemePalette::dark();
        let style = kbd_style(palette);
        assert_eq!(style.fg, Some(palette.accent));
    }

    #[test]
    fn test_score_style_high() {
        let palette = ThemePalette::dark();
        let style = score_style(9.0, palette);
        assert_eq!(style.fg, Some(colors::STATUS_SUCCESS));
    }

    #[test]
    fn test_score_style_medium() {
        let palette = ThemePalette::dark();
        let style = score_style(6.0, palette);
        assert_eq!(style.fg, Some(palette.accent));
    }

    #[test]
    fn test_score_style_low() {
        let palette = ThemePalette::dark();
        let style = score_style(3.0, palette);
        assert_eq!(style.fg, Some(palette.hint));
    }

    // ==================== RoleTheme tests ====================

    #[test]
    fn test_role_theme_has_all_fields() {
        let palette = ThemePalette::dark();
        let theme = palette.role_theme("user");
        // Verify all fields are set
        assert_ne!(theme.fg, PackedRgba::TRANSPARENT);
        assert_ne!(theme.bg, PackedRgba::TRANSPARENT);
        assert_ne!(theme.border, PackedRgba::TRANSPARENT);
        assert_ne!(theme.badge, PackedRgba::TRANSPARENT);
    }

    // ==================== PaneTheme tests ====================

    #[test]
    fn test_pane_theme_has_all_fields() {
        let pane = ThemePalette::agent_pane("claude");
        assert_ne!(pane.fg, PackedRgba::TRANSPARENT);
        assert_ne!(pane.bg, PackedRgba::TRANSPARENT);
        assert_ne!(pane.accent, PackedRgba::TRANSPARENT);
    }
}
