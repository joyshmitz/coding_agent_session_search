//! CSS generation for HTML export.
//!
//! Generates critical inline CSS that ensures the exported HTML is readable
//! even without CDN resources (Tailwind). The design matches the TUI theme.

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
        r#"
/* Search */
#search-input {
    background: var(--bg-highlight);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text-primary);
    padding: 0.5rem 1rem;
    font-size: 0.875rem;
    width: 200px;
    transition: border-color 0.2s, box-shadow 0.2s;
}
#search-input:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(122, 162, 247, 0.2);
}
#search-input::placeholder {
    color: var(--text-muted);
}
.search-count {
    font-size: 0.75rem;
    color: var(--text-muted);
    margin-left: 0.5rem;
}
.search-highlight {
    background: rgba(224, 175, 104, 0.3);
    border-radius: 2px;
    padding: 0 2px;
}
.search-current {
    background: rgba(224, 175, 104, 0.6);
}"#
    } else {
        ""
    };

    let theme_toggle_styles = if options.include_theme_toggle {
        r#"
/* Theme toggle */
.icon-sun, .icon-moon {
    width: 18px;
    height: 18px;
}
[data-theme="dark"] .icon-sun { display: none; }
[data-theme="dark"] .icon-moon { display: block; }
[data-theme="light"] .icon-sun { display: block; }
[data-theme="light"] .icon-moon { display: none; }

/* Light theme overrides */
[data-theme="light"] {
    --bg-deep: #f8f9fc;
    --bg-surface: #ffffff;
    --bg-highlight: #f0f1f5;
    --border: #e1e4eb;
    --border-focus: #5a7cc4;
    --text-primary: #1a1b26;
    --text-secondary: #3d4259;
    --text-muted: #6b7394;
    --role-user-bg: #f0f5f3;
    --role-agent-bg: #f0f2f8;
    --role-tool-bg: #f8f5f0;
    --role-system-bg: #f8f6f0;
}"#
    } else {
        ""
    };

    let encryption_styles = if options.encrypt {
        r#"
/* Modal */
.modal {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.8);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
}
.modal[hidden] {
    display: none;
}
.modal-content {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 2rem;
    max-width: 400px;
    width: 90%;
    text-align: center;
}
.modal-content h2 {
    margin: 0 0 0.5rem;
    color: var(--text-primary);
}
.modal-content p {
    color: var(--text-secondary);
    margin: 0 0 1.5rem;
}
.modal-content input {
    width: 100%;
    padding: 0.75rem 1rem;
    background: var(--bg-highlight);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text-primary);
    font-size: 1rem;
    margin-bottom: 1rem;
}
.modal-content button {
    width: 100%;
    padding: 0.75rem 1rem;
    background: var(--accent);
    color: var(--bg-deep);
    border: none;
    border-radius: 6px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity 0.2s;
}
.modal-content button:hover {
    opacity: 0.9;
}
.error {
    color: var(--status-error);
    font-size: 0.875rem;
    margin-top: 1rem;
}"#
    } else {
        ""
    };

    format!(
        r#"/* CSS Custom Properties - Tokyo Night inspired palette */
:root {{
    --bg-deep: {bg_deep};
    --bg-surface: {bg_surface};
    --bg-highlight: {bg_highlight};
    --border: {border};
    --border-focus: {border_focus};
    --text-primary: {text_primary};
    --text-secondary: {text_secondary};
    --text-muted: {text_muted};
    --text-disabled: {text_disabled};
    --accent: {accent_primary};
    --accent-secondary: {accent_secondary};
    --accent-tertiary: {accent_tertiary};
    --role-user: {role_user};
    --role-agent: {role_agent};
    --role-tool: {role_tool};
    --role-system: {role_system};
    --role-user-bg: {role_user_bg};
    --role-agent-bg: {role_agent_bg};
    --role-tool-bg: {role_tool_bg};
    --role-system-bg: {role_system_bg};
    --status-success: {status_success};
    --status-warning: {status_warning};
    --status-error: {status_error};
    --status-info: {status_info};
}}

/* Reset & Base */
*, *::before, *::after {{
    box-sizing: border-box;
}}
html {{
    font-size: 16px;
    -webkit-text-size-adjust: 100%;
}}
body {{
    margin: 0;
    padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    font-size: 1rem;
    line-height: 1.6;
    color: var(--text-primary);
    background: var(--bg-deep);
    min-height: 100vh;
}}

/* App container */
#app {{
    max-width: 900px;
    margin: 0 auto;
    padding: 2rem 1rem;
}}

/* Header */
.header {{
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid var(--border);
}}
.title {{
    font-size: 1.5rem;
    font-weight: 600;
    margin: 0 0 0.5rem;
    color: var(--text-primary);
}}
.meta {{
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    font-size: 0.875rem;
    color: var(--text-muted);
}}
.meta-item {{
    display: flex;
    align-items: center;
    gap: 0.25rem;
}}
.meta-agent {{
    color: var(--accent);
    font-weight: 500;
}}
.meta-project {{
    color: var(--accent-tertiary);
}}

/* Toolbar */
.toolbar {{
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1.5rem;
    padding: 0.75rem;
    background: var(--bg-surface);
    border-radius: 8px;
    border: 1px solid var(--border);
}}
.toolbar-item {{
    display: flex;
    align-items: center;
}}
.toolbar-btn {{
    display: flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    padding: 0;
    background: transparent;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.2s;
}}
.toolbar-btn:hover {{
    background: var(--bg-highlight);
    color: var(--text-primary);
    border-color: var(--border-focus);
}}
.toolbar-btn .icon {{
    width: 18px;
    height: 18px;
}}

/* Conversation */
.conversation {{
    display: flex;
    flex-direction: column;
    gap: 1rem;
}}

/* Message */
.message {{
    padding: 1rem 1.25rem;
    border-radius: 8px;
    border: 1px solid var(--border);
}}
.message-user {{
    background: var(--role-user-bg);
    border-left: 3px solid var(--role-user);
}}
.message-assistant, .message-agent {{
    background: var(--role-agent-bg);
    border-left: 3px solid var(--role-agent);
}}
.message-tool {{
    background: var(--role-tool-bg);
    border-left: 3px solid var(--role-tool);
}}
.message-system {{
    background: var(--role-system-bg);
    border-left: 3px solid var(--role-system);
}}

/* Message header */
.message-header {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
    font-size: 0.875rem;
}}
.message-role {{
    font-weight: 600;
    text-transform: capitalize;
}}
.message-user .message-role {{ color: var(--role-user); }}
.message-assistant .message-role, .message-agent .message-role {{ color: var(--role-agent); }}
.message-tool .message-role {{ color: var(--role-tool); }}
.message-system .message-role {{ color: var(--role-system); }}
.message-time {{
    color: var(--text-muted);
    font-size: 0.75rem;
}}

/* Message content */
.message-content {{
    color: var(--text-secondary);
}}
.message-content p {{
    margin: 0 0 0.75rem;
}}
.message-content p:last-child {{
    margin-bottom: 0;
}}

/* Code blocks */
pre {{
    background: var(--bg-deep);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem;
    overflow-x: auto;
    font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace;
    font-size: 0.875rem;
    line-height: 1.5;
    margin: 0.75rem 0;
}}
code {{
    font-family: inherit;
    background: var(--bg-highlight);
    padding: 0.125rem 0.375rem;
    border-radius: 4px;
    font-size: 0.875em;
}}
pre code {{
    background: none;
    padding: 0;
    border-radius: 0;
}}

/* Tool calls */
.tool-call {{
    margin: 0.75rem 0;
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
}}
.tool-call-header {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.75rem;
    background: var(--bg-surface);
    cursor: pointer;
    font-size: 0.875rem;
}}
.tool-call-header:hover {{
    background: var(--bg-highlight);
}}
.tool-call-name {{
    color: var(--role-tool);
    font-weight: 500;
}}
.tool-call-toggle {{
    margin-left: auto;
    color: var(--text-muted);
    transition: transform 0.2s;
}}
.tool-call.expanded .tool-call-toggle {{
    transform: rotate(180deg);
}}
.tool-call-body {{
    display: none;
    padding: 0.75rem;
    border-top: 1px solid var(--border);
}}
.tool-call.expanded .tool-call-body {{
    display: block;
}}
{search_styles}
{theme_toggle_styles}
{encryption_styles}

/* Responsive */
@media (max-width: 640px) {{
    #app {{
        padding: 1rem 0.75rem;
    }}
    .title {{
        font-size: 1.25rem;
    }}
    .message {{
        padding: 0.75rem 1rem;
    }}
    pre {{
        padding: 0.75rem;
        font-size: 0.8rem;
    }}
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
        role_agent = colors::ROLE_AGENT,
        role_tool = colors::ROLE_TOOL,
        role_system = colors::ROLE_SYSTEM,
        role_user_bg = colors::ROLE_USER_BG,
        role_agent_bg = colors::ROLE_AGENT_BG,
        role_tool_bg = colors::ROLE_TOOL_BG,
        role_system_bg = colors::ROLE_SYSTEM_BG,
        status_success = colors::STATUS_SUCCESS,
        status_warning = colors::STATUS_WARNING,
        status_error = colors::STATUS_ERROR,
        status_info = colors::STATUS_INFO,
        search_styles = search_styles,
        theme_toggle_styles = theme_toggle_styles,
        encryption_styles = encryption_styles,
    )
}

fn generate_print_css() -> String {
    r#"/* Print styles */
@page {
    margin: 0.75in;
    size: auto;
}
body {
    background: white !important;
    color: black !important;
    font-size: 11pt;
    line-height: 1.4;
}
#app {
    max-width: none;
    padding: 0;
}
.toolbar, .theme-toggle, #search-input, #theme-toggle, #print-btn {
    display: none !important;
}
.header {
    border-bottom: 2px solid #333;
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
}
.title {
    font-size: 14pt;
    color: black !important;
}
.meta {
    color: #666 !important;
}
.message {
    background: none !important;
    border: 1px solid #ccc;
    border-left-width: 3px;
    page-break-inside: avoid;
    margin-bottom: 0.5rem;
}
.message-user { border-left-color: #4a7 !important; }
.message-assistant, .message-agent { border-left-color: #47a !important; }
.message-tool { border-left-color: #a74 !important; }
.message-system { border-left-color: #a74 !important; }
pre {
    background: #f5f5f5 !important;
    border: 1px solid #ddd;
    font-size: 9pt;
    page-break-inside: avoid;
}
code {
    background: #eee !important;
}
.tool-call-body {
    display: block !important;
}
a {
    color: inherit;
    text-decoration: underline;
}
a[href^="http"]::after {
    content: " (" attr(href) ")";
    font-size: 0.8em;
    color: #666;
}"#
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
}
