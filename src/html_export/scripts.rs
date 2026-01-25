//! JavaScript generation for HTML export.
//!
//! Generates inline JavaScript for:
//! - Search functionality (text search with highlighting)
//! - Theme toggle (light/dark mode)
//! - Tool call expand/collapse
//! - Encryption/decryption (Web Crypto API)

use super::template::ExportOptions;

/// Bundle of JavaScript for the template.
pub struct ScriptBundle {
    /// Inline JavaScript to include in the document
    pub inline_js: String,
}

/// Generate all JavaScript for the template.
pub fn generate_scripts(options: &ExportOptions) -> ScriptBundle {
    let mut scripts = Vec::new();

    // Core utilities
    scripts.push(generate_core_utils());

    // Search functionality
    if options.include_search {
        scripts.push(generate_search_js());
    }

    // Theme toggle
    if options.include_theme_toggle {
        scripts.push(generate_theme_js());
    }

    // Tool call toggle
    if options.show_tool_calls {
        scripts.push(generate_tool_toggle_js());
    }

    // Encryption/decryption
    if options.encrypt {
        scripts.push(generate_decryption_js());
    }

    // Initialize on load
    scripts.push(generate_init_js(options));

    ScriptBundle {
        inline_js: scripts.join("\n\n"),
    }
}

fn generate_core_utils() -> String {
    r#"// Core utilities
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// Toast notifications
const Toast = {
    container: null,

    init() {
        this.container = document.createElement('div');
        this.container.id = 'toast-container';
        this.container.style.cssText = 'position:fixed;bottom:1rem;right:1rem;z-index:9999;display:flex;flex-direction:column;gap:0.5rem;';
        document.body.appendChild(this.container);
    },

    show(message, type = 'info') {
        if (!this.container) this.init();
        const toast = document.createElement('div');
        toast.className = 'toast toast-' + type;
        toast.style.cssText = 'padding:0.75rem 1rem;background:var(--bg-surface);border:1px solid var(--border);border-radius:6px;color:var(--text-primary);box-shadow:0 4px 12px rgba(0,0,0,0.3);transform:translateX(100%);transition:transform 0.3s ease;';
        toast.textContent = message;
        this.container.appendChild(toast);
        requestAnimationFrame(() => toast.style.transform = 'translateX(0)');
        setTimeout(() => {
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }
};

// Copy to clipboard
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        Toast.show('Copied to clipboard', 'success');
    } catch (e) {
        // Fallback for older browsers
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            Toast.show('Copied to clipboard', 'success');
        } catch (e2) {
            Toast.show('Copy failed', 'error');
        }
        textarea.remove();
    }
}

// Copy code block
function copyCodeBlock(btn) {
    const pre = btn.closest('pre');
    const code = pre.querySelector('code');
    copyToClipboard(code ? code.textContent : pre.textContent);
}

// Print handler
function printConversation() {
    // Expand all collapsed sections before print
    $$('details, .tool-call').forEach(el => {
        if (el.tagName === 'DETAILS') el.open = true;
        else el.classList.add('expanded');
    });
    window.print();
}"#
        .to_string()
}

fn generate_search_js() -> String {
    r#"// Search functionality
const Search = {
    input: null,
    countEl: null,
    matches: [],
    currentIndex: -1,

    init() {
        this.input = $('#search-input');
        this.countEl = $('#search-count');
        if (!this.input) return;

        this.input.addEventListener('input', () => this.search());
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                if (e.shiftKey) {
                    this.prev();
                } else {
                    this.next();
                }
            } else if (e.key === 'Escape') {
                this.clear();
                this.input.blur();
            }
        });

        // Keyboard shortcut: Ctrl/Cmd + F for search
        document.addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
                e.preventDefault();
                this.input.focus();
                this.input.select();
            }
        });
    },

    search() {
        this.clearHighlights();
        const query = this.input.value.trim().toLowerCase();
        if (!query) {
            this.countEl.hidden = true;
            return;
        }

        this.matches = [];
        const messages = $$('.message-content');
        messages.forEach((el) => {
            const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
            let node;
            while ((node = walker.nextNode())) {
                const text = node.textContent.toLowerCase();
                let index = text.indexOf(query);
                while (index !== -1) {
                    this.matches.push({ node, index, length: query.length });
                    index = text.indexOf(query, index + 1);
                }
            }
        });

        this.highlightAll();
        this.updateCount();

        if (this.matches.length > 0) {
            this.currentIndex = 0;
            this.scrollToCurrent();
        }
    },

    highlightAll() {
        // Process in reverse to preserve indices
        for (let i = this.matches.length - 1; i >= 0; i--) {
            const match = this.matches[i];
            const range = document.createRange();
            try {
                range.setStart(match.node, match.index);
                range.setEnd(match.node, match.index + match.length);
                const span = document.createElement('span');
                span.className = 'search-highlight';
                span.dataset.matchIndex = i;
                range.surroundContents(span);
            } catch (e) {
                // Skip invalid ranges
            }
        }
    },

    clearHighlights() {
        $$('.search-highlight').forEach((el) => {
            const parent = el.parentNode;
            while (el.firstChild) {
                parent.insertBefore(el.firstChild, el);
            }
            parent.removeChild(el);
        });
        this.matches = [];
        this.currentIndex = -1;
    },

    updateCount() {
        if (this.matches.length > 0) {
            this.countEl.textContent = `${this.currentIndex + 1}/${this.matches.length}`;
            this.countEl.hidden = false;
        } else {
            this.countEl.textContent = 'No results';
            this.countEl.hidden = false;
        }
    },

    scrollToCurrent() {
        $$('.search-current').forEach((el) => el.classList.remove('search-current'));
        if (this.currentIndex >= 0 && this.currentIndex < this.matches.length) {
            const highlight = $(`[data-match-index="${this.currentIndex}"]`);
            if (highlight) {
                highlight.classList.add('search-current');
                highlight.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }
        }
        this.updateCount();
    },

    next() {
        if (this.matches.length === 0) return;
        this.currentIndex = (this.currentIndex + 1) % this.matches.length;
        this.scrollToCurrent();
    },

    prev() {
        if (this.matches.length === 0) return;
        this.currentIndex = (this.currentIndex - 1 + this.matches.length) % this.matches.length;
        this.scrollToCurrent();
    },

    clear() {
        this.input.value = '';
        this.clearHighlights();
        this.countEl.hidden = true;
    }
};"#
        .to_string()
}

fn generate_theme_js() -> String {
    r#"// Theme toggle
const Theme = {
    toggle: null,

    init() {
        this.toggle = $('#theme-toggle');
        if (!this.toggle) return;

        // Load saved preference or system preference
        const saved = localStorage.getItem('cass-theme');
        const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        const theme = saved || (prefersDark ? 'dark' : 'light');
        document.documentElement.setAttribute('data-theme', theme);

        this.toggle.addEventListener('click', () => this.toggleTheme());

        // Listen for system theme changes
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
            if (!localStorage.getItem('cass-theme')) {
                document.documentElement.setAttribute('data-theme', e.matches ? 'dark' : 'light');
            }
        });
    },

    toggleTheme() {
        const current = document.documentElement.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('cass-theme', next);
    }
};"#
        .to_string()
}

fn generate_tool_toggle_js() -> String {
    r#"// Tool call expand/collapse
const ToolCalls = {
    init() {
        $$('.tool-call-header').forEach((header) => {
            header.addEventListener('click', () => {
                const toolCall = header.closest('.tool-call');
                toolCall.classList.toggle('expanded');
            });
        });
    }
};"#
        .to_string()
}

fn generate_decryption_js() -> String {
    r#"// Decryption using Web Crypto API
const Crypto = {
    modal: null,
    form: null,
    errorEl: null,

    init() {
        this.modal = $('#password-modal');
        this.form = $('#password-form');
        this.errorEl = $('#decrypt-error');

        if (!this.modal || !this.form) return;

        this.form.addEventListener('submit', (e) => {
            e.preventDefault();
            this.decrypt();
        });
    },

    async decrypt() {
        const password = $('#password-input').value;
        if (!password) return;

        try {
            this.errorEl.hidden = true;

            // Get encrypted content
            const encryptedEl = $('#encrypted-content');
            if (!encryptedEl) throw new Error('No encrypted content found');

            const encryptedData = JSON.parse(encryptedEl.textContent);
            const { salt, iv, ciphertext } = encryptedData;

            // Derive key from password
            const enc = new TextEncoder();
            const keyMaterial = await crypto.subtle.importKey(
                'raw',
                enc.encode(password),
                'PBKDF2',
                false,
                ['deriveBits', 'deriveKey']
            );

            const key = await crypto.subtle.deriveKey(
                {
                    name: 'PBKDF2',
                    salt: this.base64ToBuffer(salt),
                    iterations: 100000,
                    hash: 'SHA-256'
                },
                keyMaterial,
                { name: 'AES-GCM', length: 256 },
                false,
                ['decrypt']
            );

            // Decrypt
            const decrypted = await crypto.subtle.decrypt(
                {
                    name: 'AES-GCM',
                    iv: this.base64ToBuffer(iv)
                },
                key,
                this.base64ToBuffer(ciphertext)
            );

            // Replace content
            const dec = new TextDecoder();
            const plaintext = dec.decode(decrypted);
            const conversation = $('#conversation');
            conversation.innerHTML = plaintext;

            // Hide modal
            this.modal.hidden = true;

            // Re-initialize tool calls
            ToolCalls.init();

        } catch (e) {
            this.errorEl.textContent = 'Decryption failed. Wrong password?';
            this.errorEl.hidden = false;
        }
    },

    base64ToBuffer(base64) {
        const binary = atob(base64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes.buffer;
    }
};"#
        .to_string()
}

fn generate_init_js(options: &ExportOptions) -> String {
    let mut inits = Vec::new();

    if options.include_search {
        inits.push("Search.init();");
    }

    if options.include_theme_toggle {
        inits.push("Theme.init();");
    }

    if options.show_tool_calls {
        inits.push("ToolCalls.init();");
    }

    if options.encrypt {
        inits.push("Crypto.init();");
    }

    // Always add code block copy buttons and print button handler
    inits.push(r#"// Add copy buttons to code blocks
    $$('pre code').forEach((code) => {
        const pre = code.parentNode;
        const btn = document.createElement('button');
        btn.className = 'copy-code-btn';
        btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
        btn.title = 'Copy code';
        btn.onclick = () => copyCodeBlock(btn);
        btn.style.cssText = 'position:absolute;top:0.5rem;right:0.5rem;padding:0.25rem;background:var(--bg-surface);border:1px solid var(--border);border-radius:4px;color:var(--text-muted);cursor:pointer;opacity:0;transition:opacity 0.2s;';
        pre.style.position = 'relative';
        pre.appendChild(btn);
        pre.addEventListener('mouseenter', () => btn.style.opacity = '1');
        pre.addEventListener('mouseleave', () => btn.style.opacity = '0');
    });

    // Print button handler
    const printBtn = $('#print-btn');
    if (printBtn) printBtn.addEventListener('click', printConversation);

    // Global keyboard shortcut: Ctrl/Cmd + P for print
    document.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'p') {
            e.preventDefault();
            printConversation();
        }
    });"#);

    format!(
        r#"// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {{
    {}
}});"#,
        inits.join("\n    ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_scripts_includes_search() {
        let mut opts = ExportOptions::default();
        opts.include_search = true;
        let bundle = generate_scripts(&opts);

        assert!(bundle.inline_js.contains("const Search"));
        assert!(bundle.inline_js.contains("Search.init()"));
    }

    #[test]
    fn test_generate_scripts_excludes_search_when_disabled() {
        let mut opts = ExportOptions::default();
        opts.include_search = false;
        let bundle = generate_scripts(&opts);

        assert!(!bundle.inline_js.contains("const Search"));
    }

    #[test]
    fn test_generate_scripts_includes_theme_toggle() {
        let mut opts = ExportOptions::default();
        opts.include_theme_toggle = true;
        let bundle = generate_scripts(&opts);

        assert!(bundle.inline_js.contains("const Theme"));
        assert!(bundle.inline_js.contains("localStorage.getItem"));
    }

    #[test]
    fn test_generate_scripts_includes_encryption() {
        let mut opts = ExportOptions::default();
        opts.encrypt = true;
        let bundle = generate_scripts(&opts);

        assert!(bundle.inline_js.contains("const Crypto"));
        assert!(bundle.inline_js.contains("crypto.subtle"));
    }

    #[test]
    fn test_generate_scripts_includes_toast_and_copy() {
        let opts = ExportOptions::default();
        let bundle = generate_scripts(&opts);

        // Toast notifications
        assert!(bundle.inline_js.contains("const Toast"));
        assert!(bundle.inline_js.contains("Toast.show"));

        // Copy to clipboard
        assert!(bundle.inline_js.contains("copyToClipboard"));
        assert!(bundle.inline_js.contains("navigator.clipboard"));

        // Fallback for older browsers
        assert!(bundle.inline_js.contains("execCommand"));
    }

    #[test]
    fn test_generate_scripts_includes_print_handler() {
        let opts = ExportOptions::default();
        let bundle = generate_scripts(&opts);

        assert!(bundle.inline_js.contains("printConversation"));
        assert!(bundle.inline_js.contains("window.print"));
    }

    #[test]
    fn test_generate_scripts_includes_keyboard_shortcuts() {
        let mut opts = ExportOptions::default();
        opts.include_search = true;
        let bundle = generate_scripts(&opts);

        // Ctrl+F for search
        assert!(bundle.inline_js.contains("e.key === 'f'"));
        // Ctrl+P for print
        assert!(bundle.inline_js.contains("e.key === 'p'"));
        // Escape to clear
        assert!(bundle.inline_js.contains("'Escape'"));
    }

    #[test]
    fn test_generate_scripts_includes_copy_code_buttons() {
        let opts = ExportOptions::default();
        let bundle = generate_scripts(&opts);

        assert!(bundle.inline_js.contains("copy-code-btn"));
        assert!(bundle.inline_js.contains("copyCodeBlock"));
    }
}
