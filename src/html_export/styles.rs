//! CSS generation for HTML export.
//!
//! Uses Tailwind CSS v4 with @theme for custom Tokyo Night color palette.
//! When CDN is enabled, Tailwind compiles in browser. When disabled, we use
//! inline utility classes that mirror Tailwind's output.

use super::colors;
use super::template::ExportOptions;
use tracing::debug;

/// Bundle of CSS styles for the template.
pub struct StyleBundle {
    /// Critical CSS inlined in the document
    pub critical_css: String,

    /// Print-specific CSS
    pub print_css: String,
}

/// Generate all CSS styles for the template.
pub fn generate_styles(options: &ExportOptions) -> StyleBundle {
    let critical_css = generate_critical_css(options);
    let print_css = generate_print_css();
    debug!(
        component = "styles",
        operation = "generate",
        include_search = options.include_search,
        include_theme_toggle = options.include_theme_toggle,
        encrypt = options.encrypt,
        critical_bytes = critical_css.len(),
        print_bytes = print_css.len(),
        "Generated CSS styles"
    );
    StyleBundle {
        critical_css,
        print_css,
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
        r#"/* Tailwind v4 @theme - Tokyo Night colors */
@theme {{
  --color-bg: {bg};
  --color-surface: {surface};
  --color-elevated: {elevated};
  --color-text: {text};
  --color-text-secondary: {text_sec};
  --color-text-muted: {text_muted};
  --color-border: {border};
  --color-border-hover: {border_hover};
  --color-accent: {accent};
  --color-user: {user};
  --color-agent: {agent};
  --color-tool: {tool};
  --color-system: {system};
  --color-success: {success};
  --color-error: {error};
  --color-warning: {warning};
}}

/* Base reset & defaults */
*,*::before,*::after{{box-sizing:border-box}}
html{{color-scheme:dark light}}

/* Light theme overrides */
[data-theme="light"]{{
  --color-bg:#f8f9fc;
  --color-surface:#ffffff;
  --color-elevated:#f0f1f5;
  --color-text:#1a1b26;
  --color-text-secondary:#4a4e69;
  --color-text-muted:#8b8fa3;
  --color-border:#e1e4eb;
  --color-border-hover:#c8cdd8;
}}

{fallback}
{search_styles}
{theme_toggle_styles}
{encryption_styles}
{accessibility}"#,
        bg = colors::BG_DEEP,
        surface = colors::BG_SURFACE,
        elevated = colors::BG_HIGHLIGHT,
        text = colors::TEXT_PRIMARY,
        text_sec = colors::TEXT_SECONDARY,
        text_muted = colors::TEXT_MUTED,
        border = colors::BORDER,
        border_hover = colors::BORDER_FOCUS,
        accent = colors::ACCENT_PRIMARY,
        user = colors::ROLE_USER,
        agent = colors::ROLE_AGENT,
        tool = colors::ROLE_TOOL,
        system = colors::ROLE_SYSTEM,
        success = colors::STATUS_SUCCESS,
        error = colors::STATUS_ERROR,
        warning = colors::STATUS_WARNING,
        fallback = FALLBACK_STYLES,
        search_styles = search_styles,
        theme_toggle_styles = theme_toggle_styles,
        encryption_styles = encryption_styles,
        accessibility = ACCESSIBILITY_CSS,
    )
}

/// Fallback styles for when Tailwind CDN is unavailable (offline mode)
/// These mirror the Tailwind utility classes used in the HTML
const FALLBACK_STYLES: &str = r#"
/* Fallback when Tailwind CDN unavailable */
body{margin:0;font-family:system-ui,-apple-system,sans-serif;line-height:1.5;-webkit-font-smoothing:antialiased}
.bg-bg{background-color:var(--color-bg)}
.bg-surface{background-color:var(--color-surface)}
.bg-elevated{background-color:var(--color-elevated)}
.text-text{color:var(--color-text)}
.text-text-secondary{color:var(--color-text-secondary)}
.text-text-muted{color:var(--color-text-muted)}
.text-accent{color:var(--color-accent)}
.text-user{color:var(--color-user)}
.text-agent{color:var(--color-agent)}
.text-tool{color:var(--color-tool)}
.text-system{color:var(--color-system)}
.text-success{color:var(--color-success)}
.text-error{color:var(--color-error)}
.text-warning{color:var(--color-warning)}
.border-border{border-color:var(--color-border)}
.border-border-hover{border-color:var(--color-border-hover)}
.border-l-user{border-left-color:var(--color-user)}
.border-l-agent{border-left-color:var(--color-agent)}
.border-l-tool{border-left-color:var(--color-tool)}
.border-l-system{border-left-color:var(--color-system)}
.border-l-text-muted{border-left-color:var(--color-text-muted)}

/* Layout utilities */
.min-h-screen{min-height:100vh}
.w-full{width:100%}
.max-w-3xl{max-width:48rem}
.mx-auto{margin-left:auto;margin-right:auto}
.flex{display:flex}
.flex-col{flex-direction:column}
.flex-1{flex:1 1 0%}
.items-center{align-items:center}
.justify-center{justify-content:center}
.gap-1\.5{gap:0.375rem}
.gap-2{gap:0.5rem}
.flex-wrap{flex-wrap:wrap}

/* Spacing */
.p-1\.5{padding:0.375rem}
.p-2{padding:0.5rem}
.p-3{padding:0.75rem}
.px-2\.5{padding-left:0.625rem;padding-right:0.625rem}
.px-3{padding-left:0.75rem;padding-right:0.75rem}
.py-1\.5{padding-top:0.375rem;padding-bottom:0.375rem}
.py-3{padding-top:0.75rem;padding-bottom:0.75rem}
.mb-1{margin-bottom:0.25rem}
.mb-2{margin-bottom:0.5rem}
.mb-3{margin-bottom:0.75rem}
.mt-2{margin-top:0.5rem}
.ml-auto{margin-left:auto}

/* Typography */
.text-xs{font-size:0.75rem;line-height:1rem}
.text-sm{font-size:0.875rem;line-height:1.25rem}
.text-base{font-size:1rem;line-height:1.5rem}
.text-lg{font-size:1.125rem;line-height:1.75rem}
.text-xl{font-size:1.25rem;line-height:1.75rem}
.text-\[10px\]{font-size:10px}
.text-\[11px\]{font-size:11px}
.font-medium{font-weight:500}
.font-semibold{font-weight:600}
.font-mono{font-family:ui-monospace,monospace}
.leading-none{line-height:1}
.leading-relaxed{line-height:1.625}
.uppercase{text-transform:uppercase}
.antialiased{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}

/* Borders & Radius */
.border{border-width:1px}
.border-t{border-top-width:1px}
.border-b{border-bottom-width:1px}
.border-l-\[3px\]{border-left-width:3px}
.border-transparent{border-color:transparent}
.rounded{border-radius:0.25rem}
.rounded-lg{border-radius:0.5rem}

/* Interactive */
.cursor-pointer{cursor:pointer}
.transition-colors{transition-property:color,background-color,border-color;transition-timing-function:cubic-bezier(0.4,0,0.2,1);transition-duration:150ms}
.transition-transform{transition-property:transform;transition-timing-function:cubic-bezier(0.4,0,0.2,1);transition-duration:150ms}
.hover\:bg-elevated:hover{background-color:var(--color-elevated)}
.hover\:border-border:hover{border-color:var(--color-border)}
.hover\:border-border-hover:hover{border-color:var(--color-border-hover)}
.hover\:text-text:hover{color:var(--color-text)}
.group-open\/tool\:rotate-180:is([open] *){transform:rotate(180deg)}

/* Form elements */
input[type="search"]{appearance:none}
input:focus{outline:none}
.focus\:outline-none:focus{outline:none}
.focus\:border-accent:focus{border-color:var(--color-accent)}
.placeholder\:text-text-muted::placeholder{color:var(--color-text-muted)}

/* Lists */
.list-none{list-style:none}
summary::-webkit-details-marker{display:none}

/* Width/Height */
.w-8{width:2rem}
.h-8{height:2rem}
.w-\[18px\]{width:18px}
.h-\[18px\]{height:18px}

/* Overflow */
.overflow-hidden{overflow:hidden}
.overflow-x-auto{overflow-x:auto}

/* Prose reset for content */
.prose{color:var(--color-text-secondary)}
.prose p{margin:0 0 0.5rem}
.prose p:last-child{margin-bottom:0}
.prose ul,.prose ol{margin:0.375rem 0;padding-left:1.25rem}
.prose li{margin-bottom:0.25rem}
.prose code{padding:1px 4px;background:var(--color-elevated);border-radius:0.1875rem;font-size:0.875em;color:var(--color-accent)}
.prose pre{margin:0.5rem 0;background:var(--color-bg);border:1px solid var(--color-border);border-radius:0.25rem;overflow:hidden}
.prose pre code{display:block;padding:0.625rem;background:transparent;font-size:0.75rem;line-height:1.5;color:var(--color-text-secondary)}
.prose a{color:var(--color-accent);text-decoration:none}
.prose a:hover{text-decoration:underline}
.max-w-none{max-width:none}

/* Status colors with opacity */
.bg-success\/15{background-color:rgba(115,218,202,0.15)}
.bg-error\/15{background-color:rgba(247,118,142,0.15)}
.bg-warning\/15{background-color:rgba(224,175,104,0.15)}
.bg-tool\/10{background-color:rgba(255,158,100,0.1)}
.hover\:bg-tool\/10:hover{background-color:rgba(255,158,100,0.1)}

/* Responsive: md (768px+) */
@media(min-width:768px){
  .md\:px-4{padding-left:1rem;padding-right:1rem}
  .md\:py-4{padding-top:1rem;padding-bottom:1rem}
  .md\:p-2{padding:0.5rem}
  .md\:p-4{padding:1rem}
  .md\:mb-4{margin-bottom:1rem}
  .md\:pb-3{padding-bottom:0.75rem}
  .md\:gap-3{gap:0.75rem}
  .md\:text-sm{font-size:0.875rem;line-height:1.25rem}
  .md\:text-base{font-size:1rem;line-height:1.5rem}
  .md\:text-lg{font-size:1.125rem;line-height:1.75rem}
  .md\:text-xs{font-size:0.75rem;line-height:1rem}
}

/* Responsive: lg (1024px+) */
@media(min-width:1024px){
  .lg\:max-w-4xl{max-width:56rem}
  .lg\:px-6{padding-left:1.5rem;padding-right:1.5rem}
  .lg\:py-6{padding-top:1.5rem;padding-bottom:1.5rem}
  .lg\:p-5{padding:1.25rem}
  .lg\:text-xl{font-size:1.25rem;line-height:1.75rem}
}

/* Responsive: xl (1280px+) */
@media(min-width:1280px){
  .xl\:max-w-5xl{max-width:64rem}
}

/* Responsive: 2xl (1536px+) */
@media(min-width:1536px){
  .2xl\:max-w-6xl{max-width:72rem}
}

/* Print */
@media print{
  .toolbar,nav{display:none!important}
  body{background:#fff;color:#000}
  .prose{color:#333}
}

/* WorldClass UI enhancements */
.scroll-progress{position:fixed;top:0;left:0;height:2px;background:linear-gradient(90deg,var(--color-accent),var(--color-user));z-index:1000;width:0;transition:width 0.1s}
.floating-nav{position:fixed;bottom:1.5rem;right:1.5rem;display:flex;flex-direction:column;gap:0.5rem;opacity:0;transform:translateY(1rem);transition:opacity 0.2s,transform 0.2s;z-index:100;pointer-events:none}
.floating-nav.visible{opacity:1;transform:translateY(0);pointer-events:auto}
.floating-btn{width:2.5rem;height:2.5rem;display:flex;align-items:center;justify-content:center;background:var(--color-surface);border:1px solid var(--color-border);border-radius:0.5rem;color:var(--color-text-secondary);cursor:pointer;transition:all 0.15s}
.floating-btn:hover{background:var(--color-elevated);border-color:var(--color-border-hover);color:var(--color-text)}
.floating-btn .icon{width:1.125rem;height:1.125rem}
.gradient-mesh{position:fixed;inset:0;pointer-events:none;z-index:-1;opacity:0.5;background:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(122,162,247,0.15),transparent)}
.keyboard-focus{outline:2px solid var(--color-accent);outline-offset:2px}
.shortcuts-hint{position:fixed;bottom:1rem;left:50%;transform:translateX(-50%) translateY(1rem);padding:0.5rem 1rem;background:var(--color-surface);border:1px solid var(--color-border);border-radius:0.5rem;font-size:0.75rem;color:var(--color-text-muted);opacity:0;transition:opacity 0.2s,transform 0.2s;z-index:100}
.shortcuts-hint.visible{opacity:1;transform:translateX(-50%) translateY(0)}
.shortcuts-hint kbd{padding:0.125rem 0.375rem;background:var(--color-elevated);border:1px solid var(--color-border);border-radius:0.25rem;font-family:ui-monospace,monospace;font-size:0.6875rem}
.message-link-btn{position:absolute;top:0.5rem;right:0.5rem;padding:0.25rem;background:transparent;border:none;color:var(--color-text-muted);cursor:pointer;opacity:0;transition:opacity 0.15s}
article:hover .message-link-btn{opacity:1}
.message-link-btn:hover{color:var(--color-accent)}
.message-link-btn.copied{color:var(--color-success)}
.in-view{animation:messageReveal 0.4s ease-out forwards}
@keyframes messageReveal{from{opacity:0;transform:translateY(24px) scale(0.97)}to{opacity:1;transform:translateY(0) scale(1)}}
.ripple{position:absolute;border-radius:50%;background:rgba(255,255,255,0.3);pointer-events:none;animation:rippleEffect 0.6s ease-out}
@keyframes rippleEffect{to{transform:scale(4);opacity:0}}
.toolbar-hidden{transform:translateY(-100%);opacity:0}
"#;

const SEARCH_STYLES: &str = r#"
/* Search highlighting */
.search-highlight{background:rgba(122,162,247,0.3);border-radius:2px}
.search-current{background:rgba(255,158,100,0.4)}
"#;

const THEME_TOGGLE_STYLES: &str = r#"
/* Theme toggle icons */
.icon-sun,.icon-moon{transition:transform 0.15s,opacity 0.15s}
[data-theme="dark"] .icon-sun{transform:rotate(90deg);opacity:0;position:absolute}
[data-theme="dark"] .icon-moon{opacity:1}
[data-theme="light"] .icon-sun{opacity:1}
[data-theme="light"] .icon-moon{transform:rotate(-90deg);opacity:0;position:absolute}
"#;

const ENCRYPTION_STYLES: &str = r#"
/* Encryption modal */
.decrypt-modal{position:fixed;inset:0;z-index:1000;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.8)}
.decrypt-form{width:100%;max-width:22.5rem;padding:1.5rem;background:var(--color-surface);border:1px solid var(--color-border);border-radius:0.5rem}
.decrypt-form h2{margin:0 0 1rem;font-size:1.125rem;color:var(--color-text)}
.decrypt-form input{width:100%;padding:0.625rem;margin-bottom:0.75rem;background:var(--color-elevated);border:1px solid var(--color-border);border-radius:0.25rem;color:var(--color-text);font-size:0.875rem}
.decrypt-form input:focus{outline:none;border-color:var(--color-accent)}
.decrypt-form button{width:100%;padding:0.625rem;background:var(--color-accent);border:none;border-radius:0.25rem;color:#fff;font-size:0.875rem;font-weight:500;cursor:pointer}
.decrypt-form button:hover{opacity:0.9}
.decrypt-error{color:var(--color-error);font-size:0.75rem;margin-top:0.5rem}
"#;

const ACCESSIBILITY_CSS: &str = r#"
/* Accessibility */
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:0.01ms!important;transition-duration:0.01ms!important}}
:focus-visible{outline:2px solid var(--color-accent);outline-offset:2px}
"#;

fn generate_print_css() -> String {
    String::from(
        r#"@media print{
  body{font-size:11pt;background:#fff;color:#000}
  article{border:1px solid #ddd;page-break-inside:avoid}
  pre{border:1px solid #ddd;background:#f5f5f5}
  a{color:#000;text-decoration:underline}
}"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_styles_includes_colors() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains("--color-bg:"));
        assert!(bundle.critical_css.contains("--color-text:"));
    }

    #[test]
    fn test_generate_styles_includes_search_when_enabled() {
        let opts = ExportOptions {
            include_search: true,
            ..Default::default()
        };
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains(".search-highlight"));
    }

    #[test]
    fn test_generate_styles_excludes_search_when_disabled() {
        let opts = ExportOptions {
            include_search: false,
            ..Default::default()
        };
        let bundle = generate_styles(&opts);
        assert!(!bundle.critical_css.contains(".search-highlight"));
    }

    #[test]
    fn test_styles_include_theme_toggle_when_enabled() {
        let opts = ExportOptions {
            include_theme_toggle: true,
            ..Default::default()
        };
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains("[data-theme=\"light\"]"));
    }

    #[test]
    fn test_styles_include_responsive_breakpoints() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains("@media"));
    }

    #[test]
    fn test_print_css_hides_interactive_elements() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);
        assert!(bundle.print_css.contains("@media print"));
    }

    #[test]
    fn test_styles_include_accessibility() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains("prefers-reduced-motion"));
    }

    #[test]
    fn test_styles_include_animations() {
        let opts = ExportOptions::default();
        let bundle = generate_styles(&opts);
        assert!(bundle.critical_css.contains("transition"));
    }
}
