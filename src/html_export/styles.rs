//! CSS generation for HTML export.
//!
//! Terminal Noir design system with glassmorphism, glows, and premium feel.

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

    let encryption_styles = if options.encrypt {
        ENCRYPTION_STYLES
    } else {
        ""
    };

    format!(
        "{}\n{}\n{}\n{}",
        CORE_STYLES, COMPONENT_STYLES, search_styles, encryption_styles
    )
}

/// Core design system - Terminal Noir with glassmorphism
const CORE_STYLES: &str = r#"
/* ============================================
   Terminal Noir Design System v2
   World-Class Glassmorphic Interface
   ============================================ */

:root {
  /* Deep space palette - refined for depth */
  --bg-void: oklch(0.06 0.02 270);
  --bg-deep: oklch(0.09 0.018 265);
  --bg-surface: oklch(0.12 0.02 262);
  --bg-elevated: oklch(0.16 0.022 260);
  --bg-hover: oklch(0.20 0.024 258);
  --bg-glass: oklch(0.12 0.02 262 / 0.7);

  /* Text hierarchy - optimized contrast */
  --text-primary: oklch(0.96 0.005 260);
  --text-secondary: oklch(0.82 0.01 260);
  --text-muted: oklch(0.58 0.015 260);
  --text-faint: oklch(0.42 0.015 260);

  /* Borders - subtle glass edges */
  --border: oklch(0.28 0.02 260 / 0.6);
  --border-subtle: oklch(0.22 0.015 260 / 0.4);
  --border-focus: oklch(0.4 0.025 260);
  --border-glow: oklch(0.75 0.18 195 / 0.4);

  /* Accent colors - vibrant but balanced */
  --cyan: oklch(0.78 0.16 195);
  --cyan-dim: oklch(0.6 0.12 195);
  --amber: oklch(0.8 0.14 75);
  --amber-dim: oklch(0.62 0.1 75);
  --magenta: oklch(0.72 0.18 330);
  --green: oklch(0.75 0.17 145);
  --green-dim: oklch(0.58 0.12 145);
  --red: oklch(0.68 0.2 25);
  --purple: oklch(0.68 0.16 290);

  /* Role colors - distinctive and memorable */
  --role-user: var(--green);
  --role-user-glow: oklch(0.75 0.17 145 / 0.25);
  --role-agent: var(--cyan);
  --role-agent-glow: oklch(0.78 0.16 195 / 0.25);
  --role-tool: var(--amber);
  --role-tool-glow: oklch(0.8 0.14 75 / 0.2);
  --role-system: var(--purple);
  --role-system-glow: oklch(0.68 0.16 290 / 0.2);

  /* Shadows - layered depth system */
  --shadow-xs: 0 1px 2px oklch(0 0 0 / 0.15);
  --shadow-sm: 0 2px 6px oklch(0 0 0 / 0.2), 0 1px 2px oklch(0 0 0 / 0.15);
  --shadow-md: 0 4px 16px oklch(0 0 0 / 0.25), 0 2px 6px oklch(0 0 0 / 0.15);
  --shadow-lg: 0 8px 32px oklch(0 0 0 / 0.35), 0 4px 12px oklch(0 0 0 / 0.2);
  --shadow-xl: 0 16px 48px oklch(0 0 0 / 0.4), 0 8px 24px oklch(0 0 0 / 0.25);

  /* Glow effects */
  --glow-cyan: 0 0 24px oklch(0.78 0.16 195 / 0.35), 0 0 8px oklch(0.78 0.16 195 / 0.2);
  --glow-green: 0 0 24px oklch(0.75 0.17 145 / 0.35), 0 0 8px oklch(0.75 0.17 145 / 0.2);
  --glow-amber: 0 0 24px oklch(0.8 0.14 75 / 0.3), 0 0 8px oklch(0.8 0.14 75 / 0.15);

  /* Glass effect properties */
  --glass-blur: 16px;
  --glass-saturation: 1.1;

  /* Radius - consistent system */
  --radius-sm: 8px;
  --radius-md: 12px;
  --radius-lg: 16px;
  --radius-xl: 24px;
  --radius-2xl: 32px;

  /* Spacing rhythm */
  --space-xs: 4px;
  --space-sm: 8px;
  --space-md: 16px;
  --space-lg: 24px;
  --space-xl: 32px;
  --space-2xl: 48px;

  /* Transitions - physics-based easing */
  --ease-out: cubic-bezier(0.33, 1, 0.68, 1);
  --ease-in-out: cubic-bezier(0.65, 0, 0.35, 1);
  --ease-spring: cubic-bezier(0.34, 1.56, 0.64, 1);
  --transition-fast: 0.15s var(--ease-out);
  --transition-normal: 0.25s var(--ease-out);
  --transition-slow: 0.4s var(--ease-in-out);

  /* Touch targets (WCAG) */
  --touch-min: 44px;
}

/* Light mode - equally polished */
[data-theme="light"] {
  --bg-void: oklch(0.98 0.003 260);
  --bg-deep: oklch(0.97 0.005 260);
  --bg-surface: oklch(1 0 0);
  --bg-elevated: oklch(0.98 0.008 260);
  --bg-hover: oklch(0.95 0.01 260);
  --bg-glass: oklch(1 0 0 / 0.85);

  --text-primary: oklch(0.12 0.02 260);
  --text-secondary: oklch(0.28 0.025 260);
  --text-muted: oklch(0.48 0.02 260);
  --text-faint: oklch(0.62 0.015 260);

  --border: oklch(0.86 0.01 260 / 0.5);
  --border-subtle: oklch(0.9 0.008 260 / 0.4);
  --border-focus: oklch(0.75 0.015 260);

  --cyan: oklch(0.52 0.18 195);
  --green: oklch(0.48 0.16 145);
  --amber: oklch(0.55 0.16 75);

  --shadow-sm: 0 2px 6px oklch(0 0 0 / 0.06), 0 1px 2px oklch(0 0 0 / 0.04);
  --shadow-md: 0 4px 16px oklch(0 0 0 / 0.08), 0 2px 6px oklch(0 0 0 / 0.04);
  --shadow-lg: 0 8px 32px oklch(0 0 0 / 0.1), 0 4px 12px oklch(0 0 0 / 0.05);

  --glow-cyan: 0 0 20px oklch(0.52 0.18 195 / 0.2);
  --glow-green: 0 0 20px oklch(0.48 0.16 145 / 0.2);
}

/* Base reset with smooth defaults */
*, *::before, *::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

html {
  color-scheme: dark light;
  scroll-behavior: smooth;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
  font-feature-settings: "kern" 1, "liga" 1;
  hanging-punctuation: first last;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', system-ui, sans-serif;
  font-size: 15px;
  line-height: 1.65;
  color: var(--text-primary);
  background: var(--bg-void);
  min-height: 100vh;
  min-height: 100dvh;
  overflow-x: hidden;
}

/* Sophisticated multi-layer background */
body::before {
  content: '';
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: -2;
  background:
    radial-gradient(ellipse 100% 80% at 10% 20%, oklch(0.78 0.16 195 / 0.1) 0%, transparent 50%),
    radial-gradient(ellipse 80% 60% at 90% 80%, oklch(0.72 0.18 330 / 0.08) 0%, transparent 50%),
    radial-gradient(ellipse 60% 60% at 50% 50%, oklch(0.68 0.16 290 / 0.05) 0%, transparent 60%),
    radial-gradient(ellipse 120% 100% at 50% 100%, oklch(0.06 0.02 270) 0%, transparent 50%);
}

/* Subtle animated noise texture */
body::after {
  content: '';
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: -1;
  opacity: 0.4;
  mix-blend-mode: overlay;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.8' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.04'/%3E%3C/svg%3E");
}

/* ============================================
   Layout - Responsive & Spacious
   ============================================ */

.app-container {
  width: 100%;
  max-width: 720px;
  margin: 0 auto;
  padding: var(--space-md);
  padding-bottom: calc(var(--space-xl) + env(safe-area-inset-bottom, 0px));
}

/* Tablet */
@media (min-width: 768px) {
  .app-container {
    padding: var(--space-lg);
    max-width: 760px;
  }
}

/* Desktop */
@media (min-width: 1024px) {
  .app-container {
    padding: var(--space-xl);
    max-width: 840px;
  }
}

/* Large desktop */
@media (min-width: 1280px) {
  .app-container {
    max-width: 920px;
    padding: var(--space-xl) var(--space-2xl);
  }
}

/* Ultra-wide */
@media (min-width: 1536px) {
  .app-container {
    max-width: 1000px;
  }
}

/* ============================================
   Typography - Editorial Quality
   ============================================ */

h1, h2, h3, h4, h5, h6 {
  font-weight: 600;
  line-height: 1.25;
  color: var(--text-primary);
  letter-spacing: -0.02em;
  text-wrap: balance;
}

h1 { font-size: clamp(1.25rem, 4vw, 1.5rem); }
h2 { font-size: 1.25rem; }
h3 { font-size: 1.125rem; }

p {
  margin-bottom: 0.85em;
  text-wrap: pretty;
}
p:last-child { margin-bottom: 0; }

a {
  color: var(--cyan);
  text-decoration: none;
  text-underline-offset: 3px;
  transition: color var(--transition-fast), text-decoration-color var(--transition-fast);
}

a:hover {
  color: oklch(0.88 0.16 195);
  text-decoration: underline;
  text-decoration-color: oklch(0.88 0.16 195 / 0.4);
}

/* Inline code */
code {
  font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', ui-monospace, monospace;
  font-size: 0.88em;
  padding: 2px 7px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-sm);
  color: var(--cyan);
  font-feature-settings: "liga" 0;
}

/* Code blocks */
pre {
  font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', ui-monospace, monospace;
  font-size: 13px;
  line-height: 1.65;
  background: var(--bg-void);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: var(--space-md);
  overflow-x: auto;
  margin: var(--space-md) 0;
  -webkit-overflow-scrolling: touch;
}

pre code {
  padding: 0;
  background: transparent;
  border: none;
  color: var(--text-secondary);
  font-size: inherit;
}

/* Lists - better rhythm */
ul, ol {
  margin: var(--space-sm) 0;
  padding-left: 1.5em;
}
li {
  margin-bottom: 0.35em;
  padding-left: 0.25em;
}
li::marker { color: var(--text-muted); }

/* Blockquotes - distinctive */
blockquote {
  position: relative;
  border-left: 3px solid var(--cyan);
  padding: var(--space-sm) var(--space-md);
  margin: var(--space-md) 0;
  background: linear-gradient(90deg, oklch(0.78 0.16 195 / 0.05) 0%, transparent 100%);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  color: var(--text-secondary);
  font-style: italic;
}

/* Tables - refined */
table {
  width: 100%;
  border-collapse: collapse;
  margin: var(--space-md) 0;
  font-size: 14px;
}
th, td {
  padding: var(--space-sm) var(--space-md);
  border: 1px solid var(--border);
  text-align: left;
}
th {
  background: var(--bg-elevated);
  font-weight: 600;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--text-muted);
}
tr:hover td {
  background: var(--bg-surface);
}

/* Premium scrollbar */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb {
  background: var(--border);
  border-radius: 4px;
  border: 2px solid transparent;
  background-clip: padding-box;
}
::-webkit-scrollbar-thumb:hover { background: var(--border-focus); background-clip: padding-box; }

/* Firefox scrollbar */
* {
  scrollbar-width: thin;
  scrollbar-color: var(--border) transparent;
}
"#;

const COMPONENT_STYLES: &str = r#"
/* ============================================
   Header
   ============================================ */

.header {
  margin-bottom: 20px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border);
}

.header-title {
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 8px;
}

.header-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  font-size: 13px;
  color: var(--text-muted);
}

.header-meta span {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}

.header-agent {
  color: var(--cyan);
  font-weight: 500;
}

.header-project {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  padding: 2px 8px;
  background: var(--bg-elevated);
  border-radius: var(--radius-sm);
}

/* ============================================
   Toolbar - Glassmorphic
   ============================================ */

.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  margin-bottom: 20px;
  background: oklch(0.14 0.02 260 / 0.8);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border: 1px solid oklch(0.3 0.02 260 / 0.3);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-md);
}

[data-theme="light"] .toolbar {
  background: oklch(1 0 0 / 0.85);
  border-color: oklch(0.9 0.01 260);
}

.search-wrapper {
  flex: 1;
  position: relative;
}

.search-input {
  width: 100%;
  padding: 8px 12px;
  font-size: 14px;
  color: var(--text-primary);
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  outline: none;
  transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}

.search-input::placeholder { color: var(--text-faint); }

.search-input:focus {
  border-color: var(--cyan);
  box-shadow: 0 0 0 3px oklch(0.75 0.18 195 / 0.15);
}

.search-count {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 11px;
  color: var(--text-muted);
}

.toolbar-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--radius-md);
  color: var(--text-muted);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.toolbar-btn:hover {
  background: var(--bg-hover);
  border-color: var(--border);
  color: var(--text-primary);
}

.toolbar-btn svg {
  width: 18px;
  height: 18px;
}

/* Theme toggle icons */
.icon-sun, .icon-moon { transition: opacity var(--transition-fast), transform var(--transition-fast); }
[data-theme="dark"] .icon-sun { opacity: 0; position: absolute; transform: rotate(90deg); }
[data-theme="dark"] .icon-moon { opacity: 1; }
[data-theme="light"] .icon-sun { opacity: 1; }
[data-theme="light"] .icon-moon { opacity: 0; position: absolute; transform: rotate(-90deg); }

/* ============================================
   Messages
   ============================================ */

.conversation {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.message {
  position: relative;
  padding: 14px 16px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  border-left: 3px solid var(--border);
  transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}

.message:hover {
  border-color: var(--border-focus);
  box-shadow: var(--shadow-sm);
}

/* Role-specific left border colors */
.message-user { border-left-color: var(--role-user); }
.message-assistant, .message-agent { border-left-color: var(--role-agent); }
.message-tool { border-left-color: var(--role-tool); }
.message-system { border-left-color: var(--role-system); }

.message-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
  font-size: 13px;
}

.message-icon {
  font-size: 14px;
  line-height: 1;
}

.message-author {
  font-weight: 600;
  font-size: 13px;
}

.message-user .message-author { color: var(--role-user); }
.message-assistant .message-author, .message-agent .message-author { color: var(--role-agent); }
.message-tool .message-author { color: var(--role-tool); }
.message-system .message-author { color: var(--role-system); }

.message-time {
  margin-left: auto;
  font-size: 11px;
  color: var(--text-faint);
}

.message-content {
  font-size: 14px;
  line-height: 1.65;
  color: var(--text-secondary);
}

.message-content > *:first-child { margin-top: 0; }
.message-content > *:last-child { margin-bottom: 0; }

/* Message link button */
.message-link {
  position: absolute;
  top: 12px;
  right: 12px;
  opacity: 0;
  padding: 4px;
  background: transparent;
  border: none;
  color: var(--text-faint);
  cursor: pointer;
  transition: opacity var(--transition-fast), color var(--transition-fast);
}

.message:hover .message-link { opacity: 1; }
.message-link:hover { color: var(--cyan); }
.message-link.copied { color: var(--green); }

/* ============================================
   Tool Calls - Compact & Elegant
   ============================================ */

.tool-call {
  margin-top: 12px;
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  overflow: hidden;
  font-size: 13px;
}

.tool-call summary {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-elevated);
  cursor: pointer;
  list-style: none;
  transition: background var(--transition-fast);
}

.tool-call summary::-webkit-details-marker { display: none; }
.tool-call summary:hover { background: var(--bg-hover); }

.tool-call-icon { font-size: 12px; }
.tool-call-name { font-weight: 500; color: var(--amber); }

.tool-call-status {
  margin-left: auto;
  padding: 2px 8px;
  font-size: 10px;
  font-weight: 600;
  border-radius: var(--radius-sm);
}

.tool-status-success { background: oklch(0.72 0.19 145 / 0.15); color: var(--green); }
.tool-status-error { background: oklch(0.65 0.22 25 / 0.15); color: var(--red); }
.tool-status-pending { background: oklch(0.78 0.16 75 / 0.15); color: var(--amber); }

.tool-call-chevron {
  font-size: 10px;
  color: var(--text-faint);
  transition: transform var(--transition-fast);
}

.tool-call[open] .tool-call-chevron { transform: rotate(180deg); }

.tool-call-body {
  padding: 12px;
  border-top: 1px solid var(--border);
  background: var(--bg-surface);
}

.tool-call-section {
  margin-bottom: 12px;
}
.tool-call-section:last-child { margin-bottom: 0; }

.tool-call-label {
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--text-faint);
  margin-bottom: 6px;
}

.tool-call pre {
  margin: 0;
  padding: 10px;
  font-size: 11px;
  border-radius: var(--radius-sm);
}

.tool-truncated {
  font-size: 11px;
  color: var(--amber);
  margin-top: 6px;
}

/* ============================================
   Floating Navigation
   ============================================ */

.floating-nav {
  position: fixed;
  bottom: 24px;
  right: 24px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  opacity: 0;
  transform: translateY(20px);
  transition: opacity var(--transition-normal), transform var(--transition-normal);
  pointer-events: none;
  z-index: 100;
}

.floating-nav.visible {
  opacity: 1;
  transform: translateY(0);
  pointer-events: auto;
}

.floating-btn {
  width: 44px;
  height: 44px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  color: var(--text-muted);
  cursor: pointer;
  box-shadow: var(--shadow-md);
  transition: all var(--transition-fast);
}

.floating-btn:hover {
  background: var(--bg-elevated);
  border-color: var(--cyan);
  color: var(--cyan);
  box-shadow: var(--shadow-glow-cyan);
}

.floating-btn svg {
  width: 20px;
  height: 20px;
}

/* ============================================
   Scroll Progress
   ============================================ */

.scroll-progress {
  position: fixed;
  top: 0;
  left: 0;
  height: 3px;
  background: linear-gradient(90deg, var(--cyan), var(--magenta));
  z-index: 1000;
  width: 0;
  transition: width 0.1s ease;
}

/* ============================================
   Keyboard Shortcuts Hint
   ============================================ */

.shortcuts-hint {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%) translateY(20px);
  padding: 10px 16px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  font-size: 12px;
  color: var(--text-muted);
  opacity: 0;
  transition: opacity var(--transition-normal), transform var(--transition-normal);
  z-index: 100;
  box-shadow: var(--shadow-lg);
}

.shortcuts-hint.visible {
  opacity: 1;
  transform: translateX(-50%) translateY(0);
}

.shortcuts-hint kbd {
  display: inline-block;
  padding: 2px 6px;
  margin: 0 2px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 4px;
}

/* ============================================
   Animations
   ============================================ */

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}

.message {
  animation: fadeIn 0.3s ease forwards;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.6; }
}

/* ============================================
   Accessibility
   ============================================ */

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}

:focus-visible {
  outline: 2px solid var(--cyan);
  outline-offset: 2px;
}

/* ============================================
   Responsive Adjustments
   ============================================ */

@media (max-width: 640px) {
  .app-container { padding: 12px; }
  .header-title { font-size: 1rem; }
  .toolbar { padding: 8px; }
  .message { padding: 12px; }
  .message-content { font-size: 14px; }
  .tool-call pre { font-size: 10px; padding: 8px; }
  .floating-nav { bottom: 16px; right: 16px; }
  .floating-btn { width: 40px; height: 40px; }
}

/* Hide toolbar on scroll (mobile) */
.toolbar-hidden {
  transform: translateY(-100%);
  opacity: 0;
  pointer-events: none;
}

/* ============================================
   Message Collapse
   ============================================ */

.message-collapse summary {
  cursor: pointer;
  list-style: none;
}

.message-collapse summary::-webkit-details-marker { display: none; }

.message-preview {
  color: var(--text-secondary);
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.message-expand-hint {
  display: block;
  margin-top: 6px;
  font-size: 12px;
  font-weight: 500;
  color: var(--cyan);
}

.message-collapse[open] .message-expand-hint { display: none; }

.message-expanded { margin-top: 12px; }

/* ============================================
   Message Animations
   ============================================ */

@keyframes messageReveal {
  from {
    opacity: 0;
    transform: translateY(24px) scale(0.97);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}

.message.in-view {
  animation: messageReveal 0.4s ease forwards;
}

/* Keyboard focus state */
.message.keyboard-focus {
  outline: 2px solid var(--cyan);
  outline-offset: 2px;
  box-shadow: var(--shadow-glow-cyan);
}

/* ============================================
   Code Block Copy Button
   ============================================ */

pre {
  position: relative;
}

.copy-code-btn {
  position: absolute;
  top: 8px;
  right: 8px;
  padding: 4px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  cursor: pointer;
  opacity: 0;
  transition: opacity var(--transition-fast), color var(--transition-fast);
}

pre:hover .copy-code-btn { opacity: 1; }
.copy-code-btn:hover { color: var(--cyan); border-color: var(--cyan); }
.copy-code-btn.copied { color: var(--green); border-color: var(--green); }

/* ============================================
   Toast Notifications
   ============================================ */

.toast {
  padding: 10px 16px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  box-shadow: var(--shadow-lg);
  font-size: 13px;
}

.toast-success { border-color: var(--green); }
.toast-error { border-color: var(--red); }

/* ============================================
   Touch Ripple Effect
   ============================================ */

.ripple {
  position: absolute;
  border-radius: 50%;
  background: var(--cyan);
  opacity: 0.3;
  transform: scale(0);
  animation: rippleEffect 0.6s ease-out;
  pointer-events: none;
}

@keyframes rippleEffect {
  to {
    transform: scale(2.5);
    opacity: 0;
  }
}

/* ============================================
   Agent-Specific Theming
   ============================================ */

.agent-claude .message-assistant { border-left-color: oklch(0.7 0.18 50); }
.agent-codex .message-assistant { border-left-color: oklch(0.7 0.2 145); }
.agent-cursor .message-assistant { border-left-color: oklch(0.7 0.2 280); }
.agent-chatgpt .message-assistant { border-left-color: oklch(0.72 0.19 165); }
.agent-gemini .message-assistant { border-left-color: oklch(0.7 0.2 250); }
.agent-aider .message-assistant { border-left-color: oklch(0.72 0.16 85); }
.agent-copilot .message-assistant { border-left-color: oklch(0.7 0.18 200); }
.agent-cody .message-assistant { border-left-color: oklch(0.68 0.2 340); }
.agent-windsurf .message-assistant { border-left-color: oklch(0.7 0.2 205); }
.agent-amp .message-assistant { border-left-color: oklch(0.7 0.18 270); }
.agent-grok .message-assistant { border-left-color: oklch(0.7 0.22 350); }

/* Print styles */
@media print {
  body::before, body::after { display: none; }
  .toolbar, .floating-nav, .scroll-progress { display: none !important; }
  .message { break-inside: avoid; }
  .message-link { display: none; }
  .copy-code-btn { display: none; }
}
"#;

const SEARCH_STYLES: &str = r#"
/* Search highlighting */
.search-highlight {
  background: oklch(0.75 0.18 195 / 0.3);
  border-radius: 2px;
  padding: 1px 0;
}

.search-current {
  background: oklch(0.78 0.16 75 / 0.5);
}
"#;

const ENCRYPTION_STYLES: &str = r#"
/* Encryption modal */
.decrypt-modal {
  position: fixed;
  inset: 0;
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: center;
  background: oklch(0 0 0 / 0.85);
  backdrop-filter: blur(8px);
}

.decrypt-form {
  width: 100%;
  max-width: 360px;
  padding: 24px;
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-lg);
}

.decrypt-form h2 {
  margin: 0 0 16px;
  font-size: 1.125rem;
  color: var(--text-primary);
}

.decrypt-form input {
  width: 100%;
  padding: 10px 12px;
  margin-bottom: 12px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-size: 14px;
}

.decrypt-form input:focus {
  outline: none;
  border-color: var(--cyan);
}

.decrypt-form button {
  width: 100%;
  padding: 10px;
  background: var(--cyan);
  border: none;
  border-radius: var(--radius-md);
  color: var(--bg-void);
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: background var(--transition-fast);
}

.decrypt-form button:hover {
  background: oklch(0.8 0.18 195);
}

.decrypt-error {
  color: var(--red);
  font-size: 13px;
  margin-top: 8px;
}
"#;

fn generate_print_css() -> String {
    String::from(
        r#"@media print {
  body {
    font-size: 11pt;
    background: #fff;
    color: #000;
  }
  .message {
    border: 1px solid #ddd;
    page-break-inside: avoid;
  }
  pre {
    border: 1px solid #ddd;
    background: #f5f5f5;
  }
  a {
    color: #000;
    text-decoration: underline;
  }
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
        assert!(bundle.critical_css.contains("--bg-void"));
        assert!(bundle.critical_css.contains("--text-primary"));
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
        assert!(bundle.critical_css.contains("@keyframes"));
    }
}
