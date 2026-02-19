//! Text canonicalization for consistent embedding input.
//!
//! Delegates to [`frankensearch::DefaultCanonicalizer`] for the full preprocessing
//! pipeline (NFC normalization, markdown stripping, code block collapsing,
//! whitespace normalization, low-signal filtering, and truncation).
//!
//! This module adds content hashing on top of the shared canonicalization logic.
//!
//! # Example
//!
//! ```ignore
//! use crate::search::canonicalize::{canonicalize_for_embedding, content_hash};
//!
//! let raw = "**Hello** world!\n\n```rust\nfn main() {}\n```";
//! let canonical = canonicalize_for_embedding(raw);
//! let hash = content_hash(&canonical);
//! ```

use frankensearch::{Canonicalizer, DefaultCanonicalizer};
use ring::digest::{self, SHA256};

/// Maximum characters to keep after canonicalization.
pub const MAX_EMBED_CHARS: usize = 2000;

/// Maximum lines to keep from the beginning of a code block.
pub const CODE_HEAD_LINES: usize = 20;

/// Maximum lines to keep from the end of a code block.
pub const CODE_TAIL_LINES: usize = 10;

/// Canonicalize text for embedding.
///
/// Applies the full preprocessing pipeline to produce clean, consistent text
/// suitable for embedding. The output is deterministic: the same visual input
/// always produces the same output.
pub fn canonicalize_for_embedding(text: &str) -> String {
    DefaultCanonicalizer::default().canonicalize(text)
}

/// Compute SHA256 content hash of text.
///
/// The hash is computed on the UTF-8 bytes of the input. For consistent
/// hashing, always canonicalize text first.
pub fn content_hash(text: &str) -> [u8; 32] {
    let digest = digest::digest(&SHA256, text.as_bytes());
    let mut hash = [0u8; 32];
    hash.copy_from_slice(digest.as_ref());
    hash
}

/// Compute SHA256 content hash as hex string.
///
/// Convenience wrapper around [`content_hash`] that returns a hex-encoded string.
pub fn content_hash_hex(text: &str) -> String {
    let hash = content_hash(text);
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_nfc_normalization() {
        let composed = "caf\u{00E9}";
        let decomposed = "cafe\u{0301}";
        assert_ne!(composed, decomposed);
        let canon_composed = canonicalize_for_embedding(composed);
        let canon_decomposed = canonicalize_for_embedding(decomposed);
        assert_eq!(canon_composed, canon_decomposed);
    }

    #[test]
    fn test_unicode_nfc_hash_stability() {
        let composed = "caf\u{00E9}";
        let decomposed = "cafe\u{0301}";
        let hash1 = content_hash(&canonicalize_for_embedding(composed));
        let hash2 = content_hash(&canonicalize_for_embedding(decomposed));
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_canonicalize_deterministic() {
        let text = "**Hello** _world_!\n\nThis is a [link](http://example.com).";
        let result1 = canonicalize_for_embedding(text);
        let result2 = canonicalize_for_embedding(text);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_strip_markdown_bold_italic() {
        let text = "**bold** and *italic* and __also bold__";
        let canonical = canonicalize_for_embedding(text);
        assert!(!canonical.contains("**"));
        assert!(!canonical.contains("__"));
        assert!(canonical.contains("bold"));
        assert!(canonical.contains("italic"));
    }

    #[test]
    fn test_strip_markdown_links() {
        let text = "Check out [this link](http://example.com) for more info.";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("this link"));
        assert!(!canonical.contains("http://example.com"));
    }

    #[test]
    fn test_strip_markdown_headers() {
        let text = "# Header 1\n## Header 2\n### Header 3";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("Header 1"));
        assert!(canonical.contains("Header 2"));
        assert!(canonical.contains("Header 3"));
    }

    #[test]
    fn test_code_block_short() {
        let text = "```rust\nfn main() {\n    println!(\"Hello\");\n}\n```";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("[code: rust]"));
        assert!(canonical.contains("fn main()"));
    }

    #[test]
    fn test_code_block_collapse_long() {
        let mut lines = Vec::new();
        for i in 0..50 {
            lines.push(format!("line {i}"));
        }
        let code = format!("```python\n{}\n```", lines.join("\n"));
        let canonical = canonicalize_for_embedding(&code);

        assert!(canonical.contains("line 0"));
        assert!(canonical.contains("line 19"));
        assert!(canonical.contains("line 40"));
        assert!(canonical.contains("line 49"));
        assert!(canonical.contains("lines omitted"));
        assert!(!canonical.contains("line 25"));
    }

    #[test]
    fn test_whitespace_normalization() {
        let text = "hello    world\n\n\nwith   multiple   spaces";
        let canonical = canonicalize_for_embedding(text);
        assert!(!canonical.contains("  "));
        assert!(canonical.contains("hello"));
        assert!(canonical.contains("world"));
    }

    #[test]
    fn test_low_signal_filtered() {
        assert_eq!(canonicalize_for_embedding("OK"), "");
        assert_eq!(canonicalize_for_embedding("Done."), "");
        assert_eq!(canonicalize_for_embedding("Got it."), "");
        assert_eq!(canonicalize_for_embedding("Thanks!"), "Thanks!");
    }

    #[test]
    fn test_truncation() {
        let long_text: String = "a".repeat(5000);
        let canonical = canonicalize_for_embedding(&long_text);
        assert_eq!(canonical.chars().count(), 2000);
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(canonicalize_for_embedding(""), "");
    }

    #[test]
    fn test_content_hash_deterministic() {
        let text = "Hello, world!";
        let hash1 = content_hash(text);
        let hash2 = content_hash(text);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_content_hash_different_for_different_input() {
        let hash1 = content_hash("Hello");
        let hash2 = content_hash("World");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_content_hash_hex() {
        let hex = content_hash_hex("test");
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_list_markers_stripped() {
        let text = "1. First item\n2. Second item\n10. Tenth item";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("First item"));
        assert!(canonical.contains("Second item"));
        assert!(canonical.contains("Tenth item"));
    }

    #[test]
    fn test_numbers_not_list_markers_preserved() {
        let text = "3.14159 is pi";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("3.14159"));
    }

    #[test]
    fn test_blockquote() {
        let text = "> This is a quote\n> spanning multiple lines";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("This is a quote"));
    }

    #[test]
    fn test_inline_code() {
        let text = "Use `fn main()` to start.";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("fn main()"));
        assert!(!canonical.contains('`'));
    }

    #[test]
    fn test_emoji_preserved() {
        let text = "Hello 👋 World 🌍";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains('👋'));
        assert!(canonical.contains('🌍'));
    }

    #[test]
    fn test_mixed_content() {
        let text = r#"# Welcome

**Bold** and *italic* text.

```rust
fn hello() {
    println!("Hello!");
}
```

See [docs](http://docs.rs) for more.
"#;
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("Welcome"));
        assert!(!canonical.contains("**"));
        assert!(canonical.contains("Bold"));
        assert!(canonical.contains("[code: rust]"));
        assert!(canonical.contains("docs"));
        assert!(!canonical.contains("http://docs.rs"));
    }

    #[test]
    fn test_unbalanced_link_preserves_content() {
        let text = "Check [link](url( unbalanced. Next sentence.";
        let canonical = canonicalize_for_embedding(text);
        assert!(canonical.contains("Next sentence"));
        assert!(canonical.contains("unbalanced"));
    }
}
