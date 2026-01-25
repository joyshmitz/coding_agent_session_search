//! CSS generation for HTML export.
//!
//! Generates world-class, Stripe-level CSS with:
//! - Beautiful micro-interactions and animations
//! - Perfect typography hierarchy
//! - Desktop-optimized hover states and keyboard navigation
//! - Mobile-optimized touch targets and gestures
//! - Elegant depth through layering and shadows
//! - Accessibility-first design

use super::colors;
use super::template::ExportOptions;

/// Bundle of CSS styles for the template.
pub struct StyleBundle {
    /// Critical CSS inlined in the document (required for offline)
    pub critical_css: String,

    /// Print-specific CSS
    pub print_css: String,
}

/// Generate all CSS styles for the template.
pub fn generate_styles(options: &ExportOptions) -> StyleBundle {
    StyleBundle {
        critical_css: generate_critical_css(options),
        print_css: generate_print_css(),
    }
}

fn generate_critical_css(options: &ExportOptions) -> String {
    let search_styles = if options.include_search {
        SEARCH_STYLES
    } else {
        ""
    };

    let theme_toggle_styles = if options.include_theme_toggle {
        THEME_TOGGLE_STYLES
    } else {
        ""
    };

    let encryption_styles = if options.encrypt {
        ENCRYPTION_STYLES
    } else {
        ""
    };

    format!(
        r#"{base_variables}
{reset_and_base}
{typography}
{layout}
{header_styles}
{toolbar_styles}
{message_styles}
{code_block_styles}
{tool_call_styles}
{animations}
{search_styles}
{theme_toggle_styles}
{encryption_styles}
{desktop_enhancements}
{mobile_optimizations}
{accessibility}
{utility_classes}"#,
        base_variables = generate_base_variables(),
        reset_and_base = RESET_AND_BASE,
        typography = TYPOGRAPHY,
        layout = LAYOUT,
        header_styles = HEADER_STYLES,
        toolbar_styles = TOOLBAR_STYLES,
        message_styles = MESSAGE_STYLES,
        code_block_styles = CODE_BLOCK_STYLES,
        tool_call_styles = TOOL_CALL_STYLES,
        animations = ANIMATIONS,
        search_styles = search_styles,
        theme_toggle_styles = theme_toggle_styles,
        encryption_styles = encryption_styles,
        desktop_enhancements = DESKTOP_ENHANCEMENTS,
        mobile_optimizations = MOBILE_OPTIMIZATIONS,
        accessibility = ACCESSIBILITY,
        utility_classes = UTILITY_CLASSES,
    )
}

fn generate_base_variables() -> String {
    format!(
        r#"/* ============================================
   CSS Custom Properties - Tokyo Night Palette
   ============================================ */
:root {{
    /* Background layers - creates depth */
    --bg-deep: {bg_deep};
    --bg-surface: {bg_surface};
    --bg-elevated: {bg_highlight};
    --bg-overlay: rgba(26, 27, 38, 0.95);

    /* Borders with subtle gradients */
    --border: {border};
    --border-hover: {border_focus};
    --border-focus: {accent_primary};

    /* Text hierarchy */
    --text-primary: {text_primary};
    --text-secondary: {text_secondary};
    --text-muted: {text_muted};
    --text-disabled: {text_disabled};

    /* Accent colors for interactive elements */
    --accent: {accent_primary};
    --accent-hover: #8aaef9;
    --accent-pressed: #6992e5;
    --accent-secondary: {accent_secondary};
    --accent-tertiary: {accent_tertiary};

    /* Role-specific colors - conversation participants */
    --role-user: {role_user};
    --role-user-bg: {role_user_bg};
    --role-user-glow: rgba(158, 206, 106, 0.15);

    --role-agent: {role_agent};
    --role-agent-bg: {role_agent_bg};
    --role-agent-glow: rgba(122, 162, 247, 0.15);

    --role-tool: {role_tool};
    --role-tool-bg: {role_tool_bg};
    --role-tool-glow: rgba(255, 158, 100, 0.15);

    --role-system: {role_system};
    --role-system-bg: {role_system_bg};
    --role-system-glow: rgba(224, 175, 104, 0.15);

    /* Status colors */
    --success: {status_success};
    --success-bg: rgba(115, 218, 202, 0.1);
    --warning: {status_warning};
    --warning-bg: rgba(224, 175, 104, 0.1);
    --error: {status_error};
    --error-bg: rgba(247, 118, 142, 0.1);
    --info: {status_info};
    --info-bg: rgba(125, 207, 255, 0.1);

    /* Shadows - layered for depth */
    --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.1);
    --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.15), 0 2px 4px -1px rgba(0, 0, 0, 0.1);
    --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.2), 0 4px 6px -2px rgba(0, 0, 0, 0.1);
    --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.25), 0 10px 10px -5px rgba(0, 0, 0, 0.1);
    --shadow-glow: 0 0 20px rgba(122, 162, 247, 0.3);
    --shadow-inner: inset 0 2px 4px 0 rgba(0, 0, 0, 0.1);

    /* Timing functions - buttery smooth */
    --ease-out-expo: cubic-bezier(0.16, 1, 0.3, 1);
    --ease-out-back: cubic-bezier(0.34, 1.56, 0.64, 1);
    --ease-in-out: cubic-bezier(0.4, 0, 0.2, 1);
    --spring: cubic-bezier(0.175, 0.885, 0.32, 1.275);

    /* Spacing scale */
    --space-xs: 0.25rem;
    --space-sm: 0.5rem;
    --space-md: 1rem;
    --space-lg: 1.5rem;
    --space-xl: 2rem;
    --space-2xl: 3rem;
    --space-3xl: 4rem;

    /* Border radius */
    --radius-sm: 4px;
    --radius-md: 8px;
    --radius-lg: 12px;
    --radius-xl: 16px;
    --radius-full: 9999px;

    /* Z-index layers */
    --z-base: 0;
    --z-dropdown: 100;
    --z-sticky: 200;
    --z-modal: 300;
    --z-toast: 400;
    --z-tooltip: 500;
}}"#,
        bg_deep = colors::BG_DEEP,
        bg_surface = colors::BG_SURFACE,
        bg_highlight = colors::BG_HIGHLIGHT,
        border = colors::BORDER,
        border_focus = colors::BORDER_FOCUS,
        text_primary = colors::TEXT_PRIMARY,
        text_secondary = colors::TEXT_SECONDARY,
        text_muted = colors::TEXT_MUTED,
        text_disabled = colors::TEXT_DISABLED,
        accent_primary = colors::ACCENT_PRIMARY,
        accent_secondary = colors::ACCENT_SECONDARY,
        accent_tertiary = colors::ACCENT_TERTIARY,
        role_user = colors::ROLE_USER,
        role_user_bg = colors::ROLE_USER_BG,
        role_agent = colors::ROLE_AGENT,
        role_agent_bg = colors::ROLE_AGENT_BG,
        role_tool = colors::ROLE_TOOL,
        role_tool_bg = colors::ROLE_TOOL_BG,
        role_system = colors::ROLE_SYSTEM,
        role_system_bg = colors::ROLE_SYSTEM_BG,
        status_success = colors::STATUS_SUCCESS,
        status_warning = colors::STATUS_WARNING,
        status_error = colors::STATUS_ERROR,
        status_info = colors::STATUS_INFO,
    )
}

const RESET_AND_BASE: &str = r#"
/* ============================================
   Reset & Base Styles
   ============================================ */
*, *::before, *::after {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

html {
    font-size: 16px;
    -webkit-text-size-adjust: 100%;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-rendering: optimizeLegibility;
    scroll-behavior: smooth;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    font-size: 1rem;
    line-height: 1.6;
    color: var(--text-primary);
    background: var(--bg-deep);
    min-height: 100vh;
    overflow-x: hidden;
}

/* Smooth scrollbar styling */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}
::-webkit-scrollbar-track {
    background: var(--bg-deep);
}
::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: var(--radius-full);
    transition: background 0.2s;
}
::-webkit-scrollbar-thumb:hover {
    background: var(--border-hover);
}

/* Selection styling */
::selection {
    background: var(--accent);
    color: var(--bg-deep);
}
"#;

const TYPOGRAPHY: &str = r#"
/* ============================================
   Typography - Perfect Hierarchy
   ============================================ */
h1, h2, h3, h4, h5, h6 {
    font-weight: 600;
    line-height: 1.3;
    letter-spacing: -0.02em;
    color: var(--text-primary);
}

h1 { font-size: 2rem; }
h2 { font-size: 1.5rem; }
h3 { font-size: 1.25rem; }
h4 { font-size: 1.125rem; }
h5 { font-size: 1rem; }
h6 { font-size: 0.875rem; }

p {
    margin-bottom: var(--space-md);
    color: var(--text-secondary);
}

a {
    color: var(--accent);
    text-decoration: none;
    transition: color 0.2s var(--ease-out-expo);
}
a:hover {
    color: var(--accent-hover);
    text-decoration: underline;
    text-underline-offset: 2px;
}

strong, b { font-weight: 600; }
em, i { font-style: italic; }

/* Monospace font stack */
code, pre, .mono {
    font-family: 'JetBrains Mono', 'SF Mono', 'Fira Code', 'Cascadia Code', Consolas, monospace;
    font-feature-settings: 'liga' 1, 'calt' 1;
}
"#;

const LAYOUT: &str = r#"
/* ============================================
   Layout - Container & Structure
   ============================================ */
#app {
    max-width: 900px;
    margin: 0 auto;
    padding: var(--space-xl) var(--space-md);
    min-height: 100vh;
}

/* Glass morphism effect for elevated surfaces */
.glass {
    background: rgba(36, 40, 59, 0.8);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid rgba(59, 66, 97, 0.5);
}
"#;

const HEADER_STYLES: &str = r#"
/* ============================================
   Header - Session Metadata
   ============================================ */
.header {
    margin-bottom: var(--space-xl);
    padding-bottom: var(--space-lg);
    border-bottom: 1px solid var(--border);
    position: relative;
}

.header::after {
    content: '';
    position: absolute;
    bottom: -1px;
    left: 0;
    right: 0;
    height: 1px;
    background: linear-gradient(90deg, var(--accent) 0%, transparent 100%);
    opacity: 0.5;
}

.title {
    font-size: 1.75rem;
    font-weight: 700;
    margin: 0 0 var(--space-sm);
    background: linear-gradient(135deg, var(--text-primary) 0%, var(--accent) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.meta {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-md);
    font-size: 0.875rem;
    color: var(--text-muted);
}

.meta-item {
    display: inline-flex;
    align-items: center;
    gap: var(--space-xs);
}

.agent-badge {
    display: inline-flex;
    align-items: center;
    gap: var(--space-xs);
    padding: var(--space-xs) var(--space-sm);
    background: var(--role-agent-bg);
    border: 1px solid var(--role-agent);
    border-radius: var(--radius-full);
    font-weight: 500;
    font-size: 0.75rem;
    color: var(--role-agent);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    transition: all 0.2s var(--ease-out-expo);
}

.agent-badge:hover {
    background: var(--role-agent);
    color: var(--bg-deep);
    box-shadow: var(--shadow-glow);
}

/* Agent-specific badge colors */
.agent-claude { border-color: #7aa2f7; color: #7aa2f7; }
.agent-codex { border-color: #9ece6a; color: #9ece6a; }
.agent-cursor { border-color: #bb9af7; color: #bb9af7; }
.agent-chatgpt { border-color: #73daca; color: #73daca; }
.agent-gemini { border-color: #7dcfff; color: #7dcfff; }
.agent-aider { border-color: #ff9e64; color: #ff9e64; }

.workspace {
    padding: var(--space-xs) var(--space-sm);
    background: var(--bg-elevated);
    border-radius: var(--radius-sm);
    font-family: monospace;
    font-size: 0.75rem;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
"#;

const TOOLBAR_STYLES: &str = r#"
/* ============================================
   Toolbar - Actions & Controls
   ============================================ */
.toolbar {
    position: sticky;
    top: var(--space-md);
    z-index: var(--z-sticky);
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    margin-bottom: var(--space-xl);
    padding: var(--space-sm) var(--space-md);
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    box-shadow: var(--shadow-lg);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
}

.toolbar-group {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
}

.toolbar-divider {
    width: 1px;
    height: 24px;
    background: var(--border);
    margin: 0 var(--space-sm);
}

.toolbar-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    padding: 0;
    background: transparent;
    border: 1px solid transparent;
    border-radius: var(--radius-md);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.2s var(--ease-out-expo);
    position: relative;
}

.toolbar-btn::before {
    content: '';
    position: absolute;
    inset: 0;
    border-radius: inherit;
    background: var(--accent);
    opacity: 0;
    transform: scale(0.8);
    transition: all 0.2s var(--ease-out-expo);
}

.toolbar-btn:hover {
    color: var(--text-primary);
    border-color: var(--border);
    background: var(--bg-elevated);
}

.toolbar-btn:hover::before {
    opacity: 0.1;
    transform: scale(1);
}

.toolbar-btn:active {
    transform: scale(0.95);
}

.toolbar-btn .icon {
    width: 20px;
    height: 20px;
    position: relative;
    z-index: 1;
}

/* Tooltip on hover */
.toolbar-btn[data-tooltip]::after {
    content: attr(data-tooltip);
    position: absolute;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%) translateY(4px);
    padding: var(--space-xs) var(--space-sm);
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: 0.75rem;
    color: var(--text-primary);
    white-space: nowrap;
    opacity: 0;
    visibility: hidden;
    transition: all 0.2s var(--ease-out-expo);
    box-shadow: var(--shadow-md);
    z-index: var(--z-tooltip);
}

.toolbar-btn[data-tooltip]:hover::after {
    opacity: 1;
    visibility: visible;
    transform: translateX(-50%) translateY(0);
}
"#;

const MESSAGE_STYLES: &str = r#"
/* ============================================
   Messages - Conversation Flow
   ============================================ */
.conversation {
    display: flex;
    flex-direction: column;
    gap: var(--space-md);
}

.message {
    position: relative;
    padding: var(--space-lg);
    border-radius: var(--radius-lg);
    border: 1px solid var(--border);
    background: var(--bg-surface);
    transition: all 0.3s var(--ease-out-expo);
    animation: messageSlideIn 0.4s var(--ease-out-expo) backwards;
}

@keyframes messageSlideIn {
    from {
        opacity: 0;
        transform: translateY(20px);
    }
}

.message::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 3px;
    border-radius: var(--radius-lg) 0 0 var(--radius-lg);
    transition: width 0.2s var(--ease-out-expo);
}

.message:hover {
    border-color: var(--border-hover);
    box-shadow: var(--shadow-md);
}

.message:hover::before {
    width: 4px;
}

/* Role-specific styling */
.message-user {
    background: var(--role-user-bg);
}
.message-user::before {
    background: linear-gradient(180deg, var(--role-user) 0%, rgba(158, 206, 106, 0.5) 100%);
}
.message-user:hover {
    box-shadow: var(--shadow-md), 0 0 0 1px var(--role-user-glow);
}

.message-assistant, .message-agent {
    background: var(--role-agent-bg);
}
.message-assistant::before, .message-agent::before {
    background: linear-gradient(180deg, var(--role-agent) 0%, rgba(122, 162, 247, 0.5) 100%);
}
.message-assistant:hover, .message-agent:hover {
    box-shadow: var(--shadow-md), 0 0 0 1px var(--role-agent-glow);
}

.message-tool {
    background: var(--role-tool-bg);
}
.message-tool::before {
    background: linear-gradient(180deg, var(--role-tool) 0%, rgba(255, 158, 100, 0.5) 100%);
}
.message-tool:hover {
    box-shadow: var(--shadow-md), 0 0 0 1px var(--role-tool-glow);
}

.message-system {
    background: var(--role-system-bg);
}
.message-system::before {
    background: linear-gradient(180deg, var(--role-system) 0%, rgba(224, 175, 104, 0.5) 100%);
}
.message-system:hover {
    box-shadow: var(--shadow-md), 0 0 0 1px var(--role-system-glow);
}

/* Message header */
.message-header {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    margin-bottom: var(--space-md);
    padding-bottom: var(--space-sm);
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.message-role {
    font-weight: 600;
    font-size: 0.875rem;
    text-transform: capitalize;
    letter-spacing: 0.01em;
}

.message-user .message-role { color: var(--role-user); }
.message-assistant .message-role, .message-agent .message-role { color: var(--role-agent); }
.message-tool .message-role { color: var(--role-tool); }
.message-system .message-role { color: var(--role-system); }

.message-time {
    margin-left: auto;
    font-size: 0.75rem;
    color: var(--text-muted);
    font-variant-numeric: tabular-nums;
}

/* Message content */
.message-content {
    color: var(--text-secondary);
    line-height: 1.7;
}

.message-content p {
    margin-bottom: var(--space-md);
}

.message-content p:last-child {
    margin-bottom: 0;
}

.message-content ul, .message-content ol {
    margin: var(--space-md) 0;
    padding-left: var(--space-lg);
}

.message-content li {
    margin-bottom: var(--space-xs);
}
"#;

const CODE_BLOCK_STYLES: &str = r#"
/* ============================================
   Code Blocks - Syntax Highlighted
   ============================================ */
pre {
    position: relative;
    margin: var(--space-md) 0;
    padding: 0;
    background: var(--bg-deep);
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    overflow: hidden;
    transition: all 0.2s var(--ease-out-expo);
}

pre:hover {
    border-color: var(--border-hover);
    box-shadow: var(--shadow-md);
}

pre code {
    display: block;
    padding: var(--space-lg);
    overflow-x: auto;
    font-size: 0.875rem;
    line-height: 1.6;
    tab-size: 4;
    background: none;
}

/* Code header with language badge */
.code-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: var(--space-sm) var(--space-md);
    background: var(--bg-elevated);
    border-bottom: 1px solid var(--border);
}

.code-language {
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    padding: 2px 8px;
    background: var(--bg-deep);
    border-radius: var(--radius-sm);
}

/* Copy button */
.copy-code-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: var(--space-xs) var(--space-sm);
    background: transparent;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    color: var(--text-muted);
    font-size: 0.75rem;
    cursor: pointer;
    opacity: 0;
    transition: all 0.2s var(--ease-out-expo);
}

pre:hover .copy-code-btn {
    opacity: 1;
}

.copy-code-btn:hover {
    background: var(--bg-elevated);
    color: var(--text-primary);
    border-color: var(--border-hover);
}

.copy-code-btn.copied {
    color: var(--success);
    border-color: var(--success);
}

/* Inline code */
code:not(pre code) {
    padding: 2px 6px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: 0.875em;
    color: var(--accent-tertiary);
}

/* Line numbers */
.line-numbers {
    counter-reset: line;
}
.line-numbers .line::before {
    counter-increment: line;
    content: counter(line);
    display: inline-block;
    width: 3ch;
    margin-right: var(--space-md);
    text-align: right;
    color: var(--text-disabled);
    user-select: none;
}
"#;

const TOOL_CALL_STYLES: &str = r#"
/* ============================================
   Tool Calls - Collapsible Sections
   ============================================ */
.tool-call {
    margin: var(--space-md) 0;
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    overflow: hidden;
    transition: all 0.3s var(--ease-out-expo);
}

.tool-call:hover {
    border-color: var(--role-tool);
}

.tool-call-header {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-md);
    background: var(--bg-elevated);
    cursor: pointer;
    user-select: none;
    transition: background 0.2s var(--ease-out-expo);
    list-style: none;
}

.tool-call-header::-webkit-details-marker {
    display: none;
}

.tool-call-header:hover {
    background: var(--role-tool-bg);
}

.tool-call-icon {
    width: 24px;
    height: 24px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--role-tool-bg);
    border-radius: var(--radius-sm);
    color: var(--role-tool);
    font-size: 0.875rem;
}

.tool-call-name {
    font-weight: 600;
    font-size: 0.875rem;
    color: var(--role-tool);
}

.tool-call-status {
    margin-left: auto;
    display: inline-flex;
    align-items: center;
    gap: var(--space-xs);
    padding: 2px 8px;
    border-radius: var(--radius-full);
    font-size: 0.75rem;
    font-weight: 500;
}

.tool-call-status.success {
    background: var(--success-bg);
    color: var(--success);
}

.tool-call-status.error {
    background: var(--error-bg);
    color: var(--error);
}

.tool-call-toggle {
    color: var(--text-muted);
    transition: transform 0.3s var(--ease-out-expo);
}

details[open] .tool-call-toggle {
    transform: rotate(180deg);
}

.tool-call-body {
    padding: var(--space-md);
    border-top: 1px solid var(--border);
    background: var(--bg-surface);
    animation: toolBodySlideIn 0.3s var(--ease-out-expo);
}

@keyframes toolBodySlideIn {
    from {
        opacity: 0;
        transform: translateY(-8px);
    }
}

.tool-input, .tool-output {
    margin-bottom: var(--space-md);
}

.tool-input:last-child, .tool-output:last-child {
    margin-bottom: 0;
}

.tool-label {
    display: block;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    margin-bottom: var(--space-xs);
}
"#;

const ANIMATIONS: &str = r#"
/* ============================================
   Animations - Delightful Interactions
   ============================================ */
@keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
}

@keyframes slideUp {
    from {
        opacity: 0;
        transform: translateY(20px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

@keyframes slideDown {
    from {
        opacity: 0;
        transform: translateY(-20px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

@keyframes scaleIn {
    from {
        opacity: 0;
        transform: scale(0.95);
    }
    to {
        opacity: 1;
        transform: scale(1);
    }
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

@keyframes shimmer {
    0% { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}

.animate-fade-in { animation: fadeIn 0.3s var(--ease-out-expo); }
.animate-slide-up { animation: slideUp 0.4s var(--ease-out-expo); }
.animate-slide-down { animation: slideDown 0.4s var(--ease-out-expo); }
.animate-scale-in { animation: scaleIn 0.3s var(--ease-out-expo); }
.animate-pulse { animation: pulse 2s infinite; }

/* Loading skeleton */
.skeleton {
    background: linear-gradient(90deg, var(--bg-elevated) 25%, var(--bg-surface) 50%, var(--bg-elevated) 75%);
    background-size: 200% 100%;
    animation: shimmer 1.5s infinite;
    border-radius: var(--radius-sm);
}
"#;

const SEARCH_STYLES: &str = r#"
/* ============================================
   Search - Find in Conversation
   ============================================ */
.search-container {
    position: relative;
    flex: 1;
    max-width: 300px;
}

#search-input {
    width: 100%;
    padding: var(--space-sm) var(--space-md);
    padding-left: 36px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: var(--radius-full);
    color: var(--text-primary);
    font-size: 0.875rem;
    transition: all 0.2s var(--ease-out-expo);
}

#search-input:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(122, 162, 247, 0.15);
    background: var(--bg-surface);
}

#search-input::placeholder {
    color: var(--text-muted);
}

.search-icon {
    position: absolute;
    left: 12px;
    top: 50%;
    transform: translateY(-50%);
    width: 16px;
    height: 16px;
    color: var(--text-muted);
    pointer-events: none;
}

#search-count {
    position: absolute;
    right: 12px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 0.75rem;
    color: var(--text-muted);
    font-variant-numeric: tabular-nums;
}

.search-highlight {
    background: rgba(224, 175, 104, 0.3);
    border-radius: 2px;
    padding: 0 2px;
    transition: background 0.2s var(--ease-out-expo);
}

.search-current {
    background: rgba(224, 175, 104, 0.6);
    box-shadow: 0 0 0 2px var(--warning);
}

.search-nav {
    display: flex;
    gap: 2px;
    margin-left: var(--space-sm);
}

.search-nav-btn {
    width: 28px;
    height: 28px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: transparent;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    color: var(--text-muted);
    cursor: pointer;
    transition: all 0.2s var(--ease-out-expo);
}

.search-nav-btn:hover {
    background: var(--bg-elevated);
    color: var(--text-primary);
}
"#;

const THEME_TOGGLE_STYLES: &str = r#"
/* ============================================
   Theme Toggle - Light/Dark Mode
   ============================================ */
.theme-toggle {
    position: relative;
    width: 40px;
    height: 40px;
    border-radius: var(--radius-md);
    overflow: hidden;
}

.theme-toggle .icon-sun,
.theme-toggle .icon-moon {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.3s var(--ease-out-expo);
}

.theme-toggle .icon-sun svg,
.theme-toggle .icon-moon svg {
    width: 20px;
    height: 20px;
}

[data-theme="dark"] .icon-sun {
    transform: rotate(90deg) scale(0);
    opacity: 0;
}
[data-theme="dark"] .icon-moon {
    transform: rotate(0) scale(1);
    opacity: 1;
}

[data-theme="light"] .icon-sun {
    transform: rotate(0) scale(1);
    opacity: 1;
}
[data-theme="light"] .icon-moon {
    transform: rotate(-90deg) scale(0);
    opacity: 0;
}

/* Light theme overrides */
[data-theme="light"] {
    --bg-deep: #f8f9fc;
    --bg-surface: #ffffff;
    --bg-elevated: #f0f1f5;
    --bg-overlay: rgba(248, 249, 252, 0.95);
    --border: #e1e4eb;
    --border-hover: #c8cdd8;
    --border-focus: #7aa2f7;
    --text-primary: #1a1b26;
    --text-secondary: #3d4259;
    --text-muted: #6b7394;
    --text-disabled: #9da3be;
    --role-user-bg: #f0f5f3;
    --role-agent-bg: #f0f2f8;
    --role-tool-bg: #f8f5f0;
    --role-system-bg: #f8f6f0;
    --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.05);
    --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.08), 0 2px 4px -1px rgba(0, 0, 0, 0.05);
    --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

/* Smooth theme transition */
html {
    transition: background-color 0.3s var(--ease-out-expo);
}

body, .message, .toolbar, .tool-call, pre {
    transition: background-color 0.3s var(--ease-out-expo),
                border-color 0.3s var(--ease-out-expo),
                color 0.3s var(--ease-out-expo);
}
"#;

const ENCRYPTION_STYLES: &str = r#"
/* ============================================
   Encryption - Password Modal
   ============================================ */
.modal {
    position: fixed;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--bg-overlay);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    z-index: var(--z-modal);
    animation: fadeIn 0.3s var(--ease-out-expo);
}

.modal[hidden] {
    display: none;
}

.modal-content {
    width: 90%;
    max-width: 400px;
    padding: var(--space-xl);
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-xl);
    box-shadow: var(--shadow-xl);
    text-align: center;
    animation: scaleIn 0.3s var(--ease-out-back);
}

.modal-icon {
    width: 64px;
    height: 64px;
    margin: 0 auto var(--space-lg);
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--accent);
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent-secondary) 100%);
    border-radius: var(--radius-full);
    color: white;
}

.modal-icon svg {
    width: 32px;
    height: 32px;
}

.modal-title {
    margin: 0 0 var(--space-sm);
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--text-primary);
}

.modal-text {
    margin: 0 0 var(--space-lg);
    color: var(--text-secondary);
    font-size: 0.875rem;
}

.modal-input {
    width: 100%;
    padding: var(--space-md);
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: var(--radius-md);
    color: var(--text-primary);
    font-size: 1rem;
    text-align: center;
    letter-spacing: 0.1em;
    margin-bottom: var(--space-md);
    transition: all 0.2s var(--ease-out-expo);
}

.modal-input:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(122, 162, 247, 0.15);
}

.modal-btn {
    width: 100%;
    padding: var(--space-md);
    background: var(--accent);
    background: linear-gradient(135deg, var(--accent) 0%, #6992e5 100%);
    border: none;
    border-radius: var(--radius-md);
    color: white;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s var(--ease-out-expo);
}

.modal-btn:hover {
    transform: translateY(-1px);
    box-shadow: var(--shadow-lg), var(--shadow-glow);
}

.modal-btn:active {
    transform: translateY(0);
}

.modal-error {
    margin-top: var(--space-md);
    padding: var(--space-sm) var(--space-md);
    background: var(--error-bg);
    border-radius: var(--radius-md);
    color: var(--error);
    font-size: 0.875rem;
}

.encrypted-notice {
    text-align: center;
    padding: var(--space-xl);
    color: var(--text-muted);
}
"#;

const DESKTOP_ENHANCEMENTS: &str = r#"
/* ============================================
   Desktop Enhancements
   ============================================ */
@media (min-width: 768px) {
    #app {
        padding: var(--space-2xl) var(--space-xl);
    }

    .header {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: var(--space-lg);
        align-items: start;
    }

    .title {
        font-size: 2rem;
    }

    .toolbar {
        padding: var(--space-md) var(--space-lg);
    }

    /* Wide code blocks */
    pre {
        margin-left: calc(-1 * var(--space-md));
        margin-right: calc(-1 * var(--space-md));
    }

    /* Keyboard shortcut hints */
    .kbd-hint {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        margin-left: var(--space-sm);
        font-size: 0.75rem;
        color: var(--text-muted);
    }

    .kbd {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 20px;
        height: 20px;
        padding: 0 4px;
        background: var(--bg-elevated);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        font-family: inherit;
        font-size: 0.6875rem;
        font-weight: 500;
    }

    /* Message hover actions */
    .message-actions {
        position: absolute;
        top: var(--space-sm);
        right: var(--space-sm);
        display: flex;
        gap: var(--space-xs);
        opacity: 0;
        transition: opacity 0.2s var(--ease-out-expo);
    }

    .message:hover .message-actions {
        opacity: 1;
    }

    .message-action-btn {
        width: 28px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--bg-surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        color: var(--text-muted);
        cursor: pointer;
        transition: all 0.2s var(--ease-out-expo);
    }

    .message-action-btn:hover {
        background: var(--bg-elevated);
        color: var(--text-primary);
    }
}

@media (min-width: 1024px) {
    #app {
        max-width: 1000px;
    }

    /* Two-column layout for header on large screens */
    .meta {
        justify-content: flex-end;
    }
}

@media (min-width: 1280px) {
    #app {
        max-width: 1100px;
    }
}
"#;

const MOBILE_OPTIMIZATIONS: &str = r#"
/* ============================================
   Mobile Optimizations
   ============================================ */
@media (max-width: 767px) {
    html {
        font-size: 15px;
    }

    #app {
        padding: var(--space-md);
    }

    .title {
        font-size: 1.375rem;
    }

    .meta {
        flex-direction: column;
        align-items: flex-start;
        gap: var(--space-sm);
    }

    /* Floating toolbar on mobile */
    .toolbar {
        position: fixed;
        bottom: var(--space-md);
        left: var(--space-md);
        right: var(--space-md);
        top: auto;
        border-radius: var(--radius-xl);
        box-shadow: var(--shadow-xl);
        justify-content: center;
        z-index: var(--z-sticky);
    }

    /* Ensure content doesn't get hidden behind toolbar */
    .conversation {
        padding-bottom: calc(60px + var(--space-xl));
    }

    /* Larger touch targets */
    .toolbar-btn {
        width: 44px;
        height: 44px;
    }

    .toolbar-btn .icon {
        width: 22px;
        height: 22px;
    }

    /* Full-width search on mobile */
    .search-container {
        max-width: none;
    }

    /* Stack toolbar items */
    .toolbar-group {
        flex-wrap: wrap;
        justify-content: center;
    }

    .toolbar-divider {
        display: none;
    }

    /* Message adjustments */
    .message {
        padding: var(--space-md);
    }

    .message-header {
        flex-wrap: wrap;
    }

    .message-time {
        width: 100%;
        margin-top: var(--space-xs);
        margin-left: 0;
    }

    /* Code blocks */
    pre {
        border-radius: var(--radius-md);
        margin-left: calc(-1 * var(--space-md));
        margin-right: calc(-1 * var(--space-md));
        border-radius: 0;
        border-left: none;
        border-right: none;
    }

    pre code {
        padding: var(--space-md);
        font-size: 0.8125rem;
    }

    /* Tool calls */
    .tool-call-header {
        padding: var(--space-sm) var(--space-md);
    }

    /* Hide keyboard hints on mobile */
    .kbd-hint {
        display: none !important;
    }

    /* Modal adjustments */
    .modal-content {
        width: 95%;
        padding: var(--space-lg);
    }
}

/* Touch-friendly hover states */
@media (hover: none) {
    .toolbar-btn:hover::before {
        opacity: 0;
    }

    .message:hover {
        box-shadow: none;
    }

    .copy-code-btn {
        opacity: 1;
    }

    .message-actions {
        opacity: 1;
    }
}

/* Safe area insets for notched devices */
@supports (padding: max(0px)) {
    .toolbar {
        padding-left: max(var(--space-md), env(safe-area-inset-left));
        padding-right: max(var(--space-md), env(safe-area-inset-right));
        padding-bottom: max(var(--space-sm), env(safe-area-inset-bottom));
    }
}
"#;

const ACCESSIBILITY: &str = r#"
/* ============================================
   Accessibility
   ============================================ */
/* Focus visible for keyboard navigation */
:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
}

/* Remove focus outline for mouse users */
:focus:not(:focus-visible) {
    outline: none;
}

/* Skip link */
.skip-link {
    position: absolute;
    top: -100%;
    left: var(--space-md);
    padding: var(--space-sm) var(--space-md);
    background: var(--accent);
    color: white;
    border-radius: var(--radius-md);
    z-index: 9999;
    transition: top 0.2s var(--ease-out-expo);
}

.skip-link:focus {
    top: var(--space-md);
}

/* Reduced motion */
@media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
        scroll-behavior: auto !important;
    }
}

/* High contrast mode */
@media (prefers-contrast: high) {
    :root {
        --border: #ffffff;
        --text-primary: #ffffff;
        --text-secondary: #ffffff;
    }

    .message {
        border-width: 2px;
    }

    .message::before {
        width: 4px;
    }
}

/* Screen reader only */
.sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
}
"#;

const UTILITY_CLASSES: &str = r#"
/* ============================================
   Utility Classes
   ============================================ */
.hidden { display: none !important; }
.invisible { visibility: hidden !important; }
.opacity-0 { opacity: 0; }
.opacity-50 { opacity: 0.5; }
.opacity-100 { opacity: 1; }

.flex { display: flex; }
.flex-col { flex-direction: column; }
.items-center { align-items: center; }
.justify-center { justify-content: center; }
.justify-between { justify-content: space-between; }
.gap-xs { gap: var(--space-xs); }
.gap-sm { gap: var(--space-sm); }
.gap-md { gap: var(--space-md); }
.gap-lg { gap: var(--space-lg); }

.text-center { text-align: center; }
.text-left { text-align: left; }
.text-right { text-align: right; }

.font-semibold { font-weight: 600; }
.font-bold { font-weight: 700; }

.text-xs { font-size: 0.75rem; }
.text-sm { font-size: 0.875rem; }
.text-base { font-size: 1rem; }
.text-lg { font-size: 1.125rem; }
.text-xl { font-size: 1.25rem; }

.text-primary { color: var(--text-primary); }
.text-secondary { color: var(--text-secondary); }
.text-muted { color: var(--text-muted); }
.text-accent { color: var(--accent); }
.text-success { color: var(--success); }
.text-warning { color: var(--warning); }
.text-error { color: var(--error); }

.bg-deep { background: var(--bg-deep); }
.bg-surface { background: var(--bg-surface); }
.bg-elevated { background: var(--bg-elevated); }

.rounded { border-radius: var(--radius-md); }
.rounded-lg { border-radius: var(--radius-lg); }
.rounded-full { border-radius: var(--radius-full); }

.shadow { box-shadow: var(--shadow-md); }
.shadow-lg { box-shadow: var(--shadow-lg); }

.truncate {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.no-scrollbar {
    scrollbar-width: none;
    -ms-overflow-style: none;
}
.no-scrollbar::-webkit-scrollbar {
    display: none;
}
"#;

fn generate_print_css() -> String {
    r#"/* ============================================
   Print Styles - Clean PDF Output
   ============================================ */
@page {
    margin: 0.75in;
    size: auto;
}

@media print {
    * {
        -webkit-print-color-adjust: exact !important;
        print-color-adjust: exact !important;
    }

    html {
        font-size: 11pt;
    }

    body {
        background: white !important;
        color: #1a1b26 !important;
        line-height: 1.4;
    }

    #app {
        max-width: none;
        padding: 0;
    }

    /* Hide interactive elements */
    .toolbar,
    .theme-toggle,
    .copy-code-btn,
    .message-actions,
    .skip-link,
    #search-input,
    .search-nav,
    .kbd-hint {
        display: none !important;
    }

    /* Header styling */
    .header {
        border-bottom: 2px solid #1a1b26;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
    }

    .title {
        font-size: 14pt;
        background: none;
        -webkit-text-fill-color: #1a1b26;
        color: #1a1b26 !important;
    }

    .agent-badge {
        background: none;
        border: 1px solid currentColor;
    }

    .meta {
        color: #666 !important;
    }

    /* Message styling */
    .message {
        background: none !important;
        border: 1px solid #ccc;
        border-left-width: 4px;
        page-break-inside: avoid;
        margin-bottom: 0.75rem;
        box-shadow: none !important;
    }

    .message::before {
        display: none;
    }

    .message-user {
        border-left-color: #4a7c4a !important;
    }

    .message-assistant, .message-agent {
        border-left-color: #4a6fa7 !important;
    }

    .message-tool {
        border-left-color: #a77a4a !important;
    }

    .message-system {
        border-left-color: #a79a4a !important;
    }

    .message-role {
        color: #1a1b26 !important;
    }

    .message-content {
        color: #333 !important;
    }

    /* Code blocks */
    pre {
        background: #f5f5f5 !important;
        border: 1px solid #ddd;
        font-size: 9pt;
        page-break-inside: avoid;
        box-shadow: none !important;
    }

    pre code {
        white-space: pre-wrap;
        word-wrap: break-word;
    }

    code:not(pre code) {
        background: #eee !important;
        border-color: #ddd !important;
        color: #333 !important;
    }

    /* Tool calls - expanded */
    .tool-call-body {
        display: block !important;
    }

    .tool-call-toggle {
        display: none;
    }

    /* Links */
    a {
        color: inherit;
        text-decoration: underline;
    }

    a[href^="http"]::after {
        content: " (" attr(href) ")";
        font-size: 0.8em;
        color: #666;
    }

    /* Page breaks */
    h1, h2, h3 {
        page-break-after: avoid;
    }

    .conversation {
        padding-bottom: 0;
    }
}
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_styles_includes_colors() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("--bg-deep:"));
        assert!(bundle.critical_css.contains("#1a1b26"));
        assert!(bundle.critical_css.contains("--role-user:"));
        assert!(bundle.critical_css.contains("#9ece6a"));
    }

    #[test]
    fn test_generate_styles_includes_search_when_enabled() {
        let mut opts = ExportOptions::default();
        opts.include_search = true;
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("#search-input"));
        assert!(bundle.critical_css.contains(".search-highlight"));
    }

    #[test]
    fn test_generate_styles_excludes_search_when_disabled() {
        let mut opts = ExportOptions::default();
        opts.include_search = false;
        let bundle = generate_styles(&opts);

        assert!(!bundle.critical_css.contains("#search-input"));
    }

    #[test]
    fn test_print_css_hides_interactive_elements() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);

        assert!(bundle.print_css.contains(".toolbar"));
        assert!(bundle.print_css.contains("display: none"));
        assert!(bundle.print_css.contains("@page"));
    }

    #[test]
    fn test_styles_include_animations() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("@keyframes"));
        assert!(bundle.critical_css.contains("fadeIn"));
        assert!(bundle.critical_css.contains("slideUp"));
    }

    #[test]
    fn test_styles_include_responsive_breakpoints() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("@media (min-width: 768px)"));
        assert!(bundle.critical_css.contains("@media (max-width: 767px)"));
    }

    #[test]
    fn test_styles_include_accessibility() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("prefers-reduced-motion"));
        assert!(bundle.critical_css.contains("prefers-contrast"));
        assert!(bundle.critical_css.contains(".sr-only"));
        assert!(bundle.critical_css.contains(":focus-visible"));
    }

    #[test]
    fn test_styles_include_theme_toggle_when_enabled() {
        let mut opts = ExportOptions::default();
        opts.include_theme_toggle = true;
        let bundle = generate_styles(&opts);

        assert!(bundle.critical_css.contains("[data-theme=\"light\"]"));
        assert!(bundle.critical_css.contains(".theme-toggle"));
    }
}
