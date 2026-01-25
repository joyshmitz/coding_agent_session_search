//! Core HTML template generation.
//!
//! This module provides the `HtmlTemplate` struct and `HtmlExporter` for generating
//! self-contained HTML files from session data. The template follows these principles:
//!
//! - **No external template engine**: Uses Rust `format!` macros for simplicity
//! - **Critical CSS inlined**: Ensures offline functionality
//! - **CDN as enhancement**: Tailwind, Prism.js loaded with defer
//! - **Progressive enhancement**: Basic layout works without JS
//! - **Semantic HTML**: Proper use of article, section, header elements

use std::fmt;

/// Errors that can occur during template generation.
#[derive(Debug)]
pub enum TemplateError {
    /// Invalid input data
    InvalidInput(String),
    /// Rendering failed
    RenderFailed(String),
    /// Encryption required but not provided
    EncryptionRequired,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TemplateError::InvalidInput(msg) => write!(f, "invalid input: {}", msg),
            TemplateError::RenderFailed(msg) => write!(f, "render failed: {}", msg),
            TemplateError::EncryptionRequired => write!(f, "encryption required but no key provided"),
        }
    }
}

impl std::error::Error for TemplateError {}

/// Options for HTML export.
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Document title (defaults to session ID or timestamp)
    pub title: Option<String>,

    /// Include CDN resources for enhanced styling
    pub include_cdn: bool,

    /// Include syntax highlighting (Prism.js)
    pub syntax_highlighting: bool,

    /// Include search functionality
    pub include_search: bool,

    /// Include theme toggle (light/dark)
    pub include_theme_toggle: bool,

    /// Encrypt the conversation content
    pub encrypt: bool,

    /// Include print-optimized styles
    pub print_styles: bool,

    /// Agent name for branding
    pub agent_name: Option<String>,

    /// Include message timestamps
    pub show_timestamps: bool,

    /// Include tool call details (collapsed by default)
    pub show_tool_calls: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            title: None,
            include_cdn: true,
            syntax_highlighting: true,
            include_search: true,
            include_theme_toggle: true,
            encrypt: false,
            print_styles: true,
            agent_name: None,
            show_timestamps: true,
            show_tool_calls: true,
        }
    }
}

/// The HTML template structure.
///
/// Contains all the parts needed to generate a complete HTML document.
pub struct HtmlTemplate {
    /// Document title
    pub title: String,

    /// Critical inline CSS (required for offline)
    pub critical_css: String,

    /// Print-specific CSS
    pub print_css: String,

    /// Inline JavaScript
    pub inline_js: String,

    /// Main content HTML
    pub content: String,

    /// Whether content is encrypted
    pub encrypted: bool,

    /// Metadata for the header section
    pub metadata: TemplateMetadata,
}

/// Metadata displayed in the document header.
#[derive(Debug, Clone, Default)]
pub struct TemplateMetadata {
    /// Session date/time
    pub timestamp: Option<String>,

    /// Agent type (Claude, Codex, etc.)
    pub agent: Option<String>,

    /// Message count
    pub message_count: usize,

    /// Duration of session
    pub duration: Option<String>,

    /// Source project/directory
    pub project: Option<String>,
}

impl HtmlTemplate {
    /// Generate the complete HTML document.
    pub fn render(&self, options: &ExportOptions) -> String {
        let cdn_scripts = if options.include_cdn {
            r#"
    <!-- CDN enhancement (Tailwind) - degrades gracefully if offline -->
    <script src="https://cdn.tailwindcss.com" defer></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/prismjs@1/themes/prism-tomorrow.min.css" crossorigin="anonymous">
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/prism.min.js" defer crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-rust.min.js" defer crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-python.min.js" defer crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-javascript.min.js" defer crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-typescript.min.js" defer crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-bash.min.js" defer crossorigin="anonymous"></script>"#
        } else {
            ""
        };

        let print_styles = if options.print_styles {
            format!(r#"
    <style media="print">
{}
    </style>"#, self.print_css)
        } else {
            String::new()
        };

        let password_modal = if self.encrypted {
            r#"
        <!-- Password modal for encrypted content -->
        <div id="password-modal" class="modal" role="dialog" aria-labelledby="modal-title" aria-modal="true">
            <div class="modal-content">
                <h2 id="modal-title">Enter Password</h2>
                <p>This conversation is encrypted. Enter the password to view.</p>
                <form id="password-form">
                    <input type="password" id="password-input" placeholder="Password" autocomplete="current-password" required>
                    <button type="submit">Decrypt</button>
                </form>
                <p id="decrypt-error" class="error" hidden></p>
            </div>
        </div>"#
        } else {
            ""
        };

        let toolbar = self.render_toolbar(options);
        let header = self.render_header();

        format!(
            r#"<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="color-scheme" content="dark light">
    <meta name="generator" content="CASS HTML Export">
    <title>{title}</title>
    <!-- Critical inline styles for offline operation -->
    <style>
{critical_css}
    </style>{cdn_scripts}{print_styles}
</head>
<body>
    <div id="app">
{header}
{toolbar}
        <!-- Conversation container -->
        <main id="conversation" class="conversation" role="main">
{content}
        </main>
{password_modal}
    </div>
    <!-- Scripts at end for performance -->
    <script>
{inline_js}
    </script>
</body>
</html>"#,
            title = html_escape(&self.title),
            critical_css = self.critical_css,
            cdn_scripts = cdn_scripts,
            print_styles = print_styles,
            header = header,
            toolbar = toolbar,
            content = self.content,
            password_modal = password_modal,
            inline_js = self.inline_js,
        )
    }

    fn render_header(&self) -> String {
        let mut meta_items = Vec::new();

        if let Some(ts) = &self.metadata.timestamp {
            let escaped_ts = html_escape(ts);
            meta_items.push(format!(r#"<span class="meta-item"><time datetime="{}">{}</time></span>"#, escaped_ts, escaped_ts));
        }

        if let Some(agent) = &self.metadata.agent {
            meta_items.push(format!(r#"<span class="meta-item meta-agent">{}</span>"#, html_escape(agent)));
        }

        if self.metadata.message_count > 0 {
            meta_items.push(format!(r#"<span class="meta-item">{} messages</span>"#, self.metadata.message_count));
        }

        if let Some(duration) = &self.metadata.duration {
            meta_items.push(format!(r#"<span class="meta-item">{}</span>"#, html_escape(duration)));
        }

        if let Some(project) = &self.metadata.project {
            meta_items.push(format!(r#"<span class="meta-item meta-project">{}</span>"#, html_escape(project)));
        }

        let meta_html = if meta_items.is_empty() {
            String::new()
        } else {
            format!(r#"
            <div class="meta">{}</div>"#, meta_items.join("\n                "))
        };

        format!(
            r#"        <!-- Header with metadata -->
        <header class="header" role="banner">
            <h1 class="title">{}</h1>{}
        </header>"#,
            html_escape(&self.title),
            meta_html
        )
    }

    fn render_toolbar(&self, options: &ExportOptions) -> String {
        let mut toolbar_items = Vec::new();

        if options.include_search {
            toolbar_items.push(r#"<div class="toolbar-item">
                <input type="search" id="search-input" placeholder="Search..." aria-label="Search conversation">
                <span id="search-count" class="search-count" hidden></span>
            </div>"#.to_string());
        }

        if options.include_theme_toggle {
            toolbar_items.push(r#"<button id="theme-toggle" class="toolbar-btn" aria-label="Toggle theme" title="Toggle light/dark theme">
                <svg class="icon icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="5"/>
                    <path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>
                </svg>
                <svg class="icon icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
                </svg>
            </button>"#.to_string());
        }

        toolbar_items.push(r#"<button id="print-btn" class="toolbar-btn" aria-label="Print" title="Print conversation">
                <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M6 9V2h12v7M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/>
                    <rect x="6" y="14" width="12" height="8"/>
                </svg>
            </button>"#.to_string());

        if toolbar_items.is_empty() {
            return String::new();
        }

        format!(
            r#"        <!-- Toolbar -->
        <nav class="toolbar" role="navigation" aria-label="Conversation tools">
            {}
        </nav>"#,
            toolbar_items.join("\n            ")
        )
    }
}

/// Main exporter for generating HTML from sessions.
pub struct HtmlExporter {
    options: ExportOptions,
}

impl HtmlExporter {
    /// Create a new exporter with default options.
    pub fn new() -> Self {
        Self {
            options: ExportOptions::default(),
        }
    }

    /// Create a new exporter with custom options.
    pub fn with_options(options: ExportOptions) -> Self {
        Self { options }
    }

    /// Get the current options.
    pub fn options(&self) -> &ExportOptions {
        &self.options
    }

    /// Generate an empty template for testing.
    pub fn create_template(&self, title: &str) -> HtmlTemplate {
        use super::scripts::generate_scripts;
        use super::styles::generate_styles;

        let styles = generate_styles(&self.options);
        let scripts = generate_scripts(&self.options);

        HtmlTemplate {
            title: title.to_string(),
            critical_css: styles.critical_css,
            print_css: styles.print_css,
            inline_js: scripts.inline_js,
            content: String::new(),
            encrypted: self.options.encrypt,
            metadata: TemplateMetadata::default(),
        }
    }
}

impl Default for HtmlExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Escape HTML special characters.
pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a & b"), "a &amp; b");
        assert_eq!(html_escape(r#"say "hello""#), "say &quot;hello&quot;");
    }

    #[test]
    fn test_export_options_default() {
        let opts = ExportOptions::default();
        assert!(opts.include_cdn);
        assert!(opts.syntax_highlighting);
        assert!(!opts.encrypt);
    }

    #[test]
    fn test_template_renders_valid_html() {
        let template = HtmlTemplate {
            title: "Test Session".to_string(),
            critical_css: "body { background: #1a1b26; }".to_string(),
            print_css: "@page { margin: 1in; }".to_string(),
            inline_js: "console.log('loaded');".to_string(),
            content: "<p>Hello, World!</p>".to_string(),
            encrypted: false,
            metadata: TemplateMetadata::default(),
        };

        let html = template.render(&ExportOptions::default());

        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("<html lang=\"en\""));
        assert!(html.contains("Test Session"));
        assert!(html.contains("Hello, World!"));
        assert!(html.contains("background: #1a1b26"));
    }

    #[test]
    fn test_encrypted_template_shows_modal() {
        let template = HtmlTemplate {
            title: "Encrypted".to_string(),
            critical_css: String::new(),
            print_css: String::new(),
            inline_js: String::new(),
            content: "[ENCRYPTED]".to_string(),
            encrypted: true,
            metadata: TemplateMetadata::default(),
        };

        let html = template.render(&ExportOptions::default());
        assert!(html.contains("password-modal"));
        assert!(html.contains("Enter Password"));
    }
}
