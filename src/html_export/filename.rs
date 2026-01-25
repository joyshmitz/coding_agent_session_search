//! Smart filename generation for HTML exports.
//!
//! Generates cross-platform safe filenames from session metadata,
//! ensuring compatibility with Windows, macOS, and Linux filesystems.

use std::path::PathBuf;

/// Options for filename generation.
#[derive(Debug, Clone, Default)]
pub struct FilenameOptions {
    /// Include date in filename
    pub include_date: bool,

    /// Include agent name in filename
    pub include_agent: bool,

    /// Include project name in filename
    pub include_project: bool,

    /// Include topic in filename (if provided)
    pub include_topic: bool,

    /// Maximum filename length (excluding extension)
    pub max_length: Option<usize>,

    /// Custom prefix
    pub prefix: Option<String>,

    /// Custom suffix (before extension)
    pub suffix: Option<String>,
}

/// Metadata for filename generation.
#[derive(Debug, Clone, Default)]
pub struct FilenameMetadata {
    /// Session title or ID
    pub title: Option<String>,

    /// ISO date (YYYY-MM-DD)
    pub date: Option<String>,

    /// Agent name (claude, codex, etc.)
    pub agent: Option<String>,

    /// Project name
    pub project: Option<String>,

    /// Topic provided by calling agent (robot mode).
    /// Will be normalized to lowercase with underscores.
    pub topic: Option<String>,
}

/// Normalize a topic string to lowercase with underscores.
///
/// This is the canonical way to convert a user-provided topic
/// into the format expected by CASS filenames:
/// - Converts to lowercase
/// - Replaces spaces with underscores
/// - Removes invalid characters
/// - Collapses multiple underscores
///
/// # Examples
/// ```
/// use cass::html_export::filename::normalize_topic;
/// assert_eq!(normalize_topic("My Cool Topic"), "my_cool_topic");
/// assert_eq!(normalize_topic("HTML Export Feature"), "html_export_feature");
/// ```
pub fn normalize_topic(topic: &str) -> String {
    sanitize(topic)
}

/// Generate a safe, descriptive filename.
///
/// Returns a filename without extension (add .html manually).
pub fn generate_filename(metadata: &FilenameMetadata, options: &FilenameOptions) -> String {
    let mut parts = Vec::new();

    // Add prefix
    if let Some(prefix) = &options.prefix {
        parts.push(sanitize(prefix));
    }

    // Add date
    if options.include_date {
        if let Some(date) = &metadata.date {
            parts.push(sanitize(date));
        }
    }

    // Add agent
    if options.include_agent {
        if let Some(agent) = &metadata.agent {
            parts.push(sanitize(agent));
        }
    }

    // Add project
    if options.include_project {
        if let Some(project) = &metadata.project {
            parts.push(sanitize(project));
        }
    }

    // Add topic (robot mode can supply this for intelligent naming)
    if options.include_topic {
        if let Some(topic) = &metadata.topic {
            parts.push(normalize_topic(topic));
        }
    }

    // Add title (always included if present)
    if let Some(title) = &metadata.title {
        parts.push(sanitize(title));
    }

    // Add suffix
    if let Some(suffix) = &options.suffix {
        parts.push(sanitize(suffix));
    }

    // Combine parts
    let mut filename = if parts.is_empty() {
        "session".to_string()
    } else {
        parts.join("_")
    };

    // Apply max length
    if let Some(max_len) = options.max_length {
        if filename.len() > max_len {
            filename = filename[..max_len].to_string();
            // Trim trailing underscores or hyphens
            filename = filename.trim_end_matches(|c| c == '_' || c == '-').to_string();
        }
    }

    filename
}

/// Generate a filename with path.
pub fn generate_filepath(
    base_dir: &std::path::Path,
    metadata: &FilenameMetadata,
    options: &FilenameOptions,
) -> PathBuf {
    let filename = generate_filename(metadata, options);
    base_dir.join(format!("{}.html", filename))
}

/// Sanitize a string for use in filenames.
///
/// - Replaces invalid characters with underscores
/// - Removes leading/trailing whitespace
/// - Collapses multiple underscores
/// - Limits to ASCII alphanumeric plus safe punctuation
fn sanitize(s: &str) -> String {
    let mut result = String::new();
    let mut last_was_underscore = false;

    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '-' {
            result.push(c.to_ascii_lowercase());
            last_was_underscore = false;
        } else if c == ' ' || c == '_' || c == '.' || c == '/' || c == '\\' {
            // Replace separators with underscore, avoiding duplicates
            if !last_was_underscore && !result.is_empty() {
                result.push('_');
                last_was_underscore = true;
            }
        }
        // Skip other characters
    }

    // Trim leading/trailing underscores
    result.trim_matches('_').to_string()
}

/// Characters that are invalid in filenames across platforms.
const INVALID_CHARS: &[char] = &[
    '<', '>', ':', '"', '/', '\\', '|', '?', '*',
    '\0', '\n', '\r', '\t',
];

/// Reserved filenames on Windows.
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Check if a filename is valid across platforms.
pub fn is_valid_filename(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    // Check for invalid characters
    if name.chars().any(|c| INVALID_CHARS.contains(&c)) {
        return false;
    }

    // Check for reserved names (Windows)
    let upper = name.to_ascii_uppercase();
    let base_name = upper.split('.').next().unwrap_or(&upper);
    if RESERVED_NAMES.contains(&base_name) {
        return false;
    }

    // Check for leading/trailing spaces or dots
    if name.starts_with(' ') || name.starts_with('.') ||
       name.ends_with(' ') || name.ends_with('.') {
        return false;
    }

    // Check length (Windows MAX_PATH is 260, but NTFS supports 255 per component)
    if name.len() > 255 {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_basic() {
        assert_eq!(sanitize("Hello World"), "hello_world");
        assert_eq!(sanitize("test.file"), "test_file");
        assert_eq!(sanitize("path/to/file"), "path_to_file");
    }

    #[test]
    fn test_sanitize_special_chars() {
        assert_eq!(sanitize("file<>:name"), "filename");
        assert_eq!(sanitize("test?*file"), "testfile");
    }

    #[test]
    fn test_sanitize_multiple_separators() {
        assert_eq!(sanitize("hello   world"), "hello_world");
        assert_eq!(sanitize("test___file"), "test_file");
    }

    #[test]
    fn test_generate_filename_basic() {
        let meta = FilenameMetadata {
            title: Some("My Session".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions::default();

        assert_eq!(generate_filename(&meta, &opts), "my_session");
    }

    #[test]
    fn test_generate_filename_with_date() {
        let meta = FilenameMetadata {
            title: Some("Session".to_string()),
            date: Some("2026-01-25".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions {
            include_date: true,
            ..Default::default()
        };

        let result = generate_filename(&meta, &opts);
        assert!(result.starts_with("2026-01-25"));
        assert!(result.contains("session"));
    }

    #[test]
    fn test_generate_filename_max_length() {
        let meta = FilenameMetadata {
            title: Some("A very long session title that exceeds limits".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions {
            max_length: Some(20),
            ..Default::default()
        };

        let result = generate_filename(&meta, &opts);
        assert!(result.len() <= 20);
    }

    #[test]
    fn test_generate_filename_empty() {
        let meta = FilenameMetadata::default();
        let opts = FilenameOptions::default();

        assert_eq!(generate_filename(&meta, &opts), "session");
    }

    #[test]
    fn test_is_valid_filename() {
        assert!(is_valid_filename("valid_file.txt"));
        assert!(is_valid_filename("test-123"));

        assert!(!is_valid_filename(""));
        assert!(!is_valid_filename("file<name"));
        assert!(!is_valid_filename("CON")); // Reserved on Windows
        assert!(!is_valid_filename(".hidden")); // Leading dot
    }

    #[test]
    fn test_generate_filepath() {
        let meta = FilenameMetadata {
            title: Some("test".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions::default();
        let path = generate_filepath(std::path::Path::new("/tmp"), &meta, &opts);

        assert_eq!(path, PathBuf::from("/tmp/test.html"));
    }

    #[test]
    fn test_normalize_topic_basic() {
        assert_eq!(normalize_topic("My Cool Topic"), "my_cool_topic");
        assert_eq!(normalize_topic("HTML Export Feature"), "html_export_feature");
        assert_eq!(normalize_topic("debugging auth flow"), "debugging_auth_flow");
    }

    #[test]
    fn test_normalize_topic_special_chars() {
        // Special characters should be removed
        assert_eq!(normalize_topic("API Design (v2)"), "api_design_v2");
        assert_eq!(normalize_topic("fix: login bug"), "fix_login_bug");
        assert_eq!(normalize_topic("add feature #123"), "add_feature_123");
    }

    #[test]
    fn test_normalize_topic_already_normalized() {
        // Already normalized topics should pass through
        assert_eq!(normalize_topic("already_normalized"), "already_normalized");
        assert_eq!(normalize_topic("lowercase_topic"), "lowercase_topic");
    }

    #[test]
    fn test_normalize_topic_multiple_spaces() {
        // Multiple spaces should collapse to single underscore
        assert_eq!(normalize_topic("too   many    spaces"), "too_many_spaces");
    }

    #[test]
    fn test_generate_filename_with_topic() {
        let meta = FilenameMetadata {
            date: Some("2026-01-25".to_string()),
            agent: Some("claude".to_string()),
            topic: Some("Debugging Auth Flow".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions {
            include_date: true,
            include_agent: true,
            include_topic: true,
            ..Default::default()
        };

        let result = generate_filename(&meta, &opts);
        assert!(result.contains("2026-01-25"));
        assert!(result.contains("claude"));
        assert!(result.contains("debugging_auth_flow"));
    }

    #[test]
    fn test_generate_filename_topic_without_flag() {
        // Topic should not appear if include_topic is false
        let meta = FilenameMetadata {
            topic: Some("My Topic".to_string()),
            title: Some("Session".to_string()),
            ..Default::default()
        };
        let opts = FilenameOptions {
            include_topic: false,
            ..Default::default()
        };

        let result = generate_filename(&meta, &opts);
        assert!(!result.contains("my_topic"));
        assert_eq!(result, "session");
    }

    #[test]
    fn test_generate_filename_full_robot_mode() {
        // Typical robot mode export with all metadata
        let meta = FilenameMetadata {
            date: Some("2026-01-25".to_string()),
            agent: Some("claude_code".to_string()),
            project: Some("my-project".to_string()),
            topic: Some("Fix Authentication Bug".to_string()),
            title: None, // Robot mode might not use title
        };
        let opts = FilenameOptions {
            include_date: true,
            include_agent: true,
            include_project: true,
            include_topic: true,
            ..Default::default()
        };

        let result = generate_filename(&meta, &opts);
        // Should produce something like: 2026-01-25_claude_code_my-project_fix_authentication_bug
        assert!(result.starts_with("2026-01-25"));
        assert!(result.contains("claude_code"));
        assert!(result.contains("my-project"));
        assert!(result.contains("fix_authentication_bug"));
    }
}
