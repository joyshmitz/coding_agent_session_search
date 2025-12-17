//! Configuration types for remote sources.
//!
//! This module defines the data structures for configuring remote sources
//! that cass can sync agent sessions from. Configuration is stored in TOML
//! format at `~/.config/cass/sources.toml` (or XDG equivalent).
//!
//! # Example Configuration
//!
//! ```toml
//! [[sources]]
//! name = "laptop"
//! type = "ssh"
//! host = "user@laptop.local"
//! paths = ["~/.claude/projects", "~/.cursor"]
//! sync_schedule = "manual"
//!
//! [[sources]]
//! name = "workstation"
//! type = "ssh"
//! host = "user@work.example.com"
//! paths = ["~/.claude/projects"]
//! sync_schedule = "daily"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;

use super::provenance::SourceKind;

/// Errors that can occur when loading or saving source configuration.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Read(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Failed to serialize config: {0}")]
    Serialize(#[from] toml::ser::Error),

    #[error("Could not determine config directory")]
    NoConfigDir,

    #[error("Validation error: {0}")]
    Validation(String),
}

/// Root configuration containing all source definitions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcesConfig {
    /// List of configured sources.
    #[serde(default)]
    pub sources: Vec<SourceDefinition>,
}

/// Definition of a single source (local or remote).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceDefinition {
    /// Friendly name for this source (e.g., "laptop", "workstation").
    /// This becomes the `source_id` used throughout the system.
    pub name: String,

    /// Connection type (local, ssh, etc.).
    #[serde(rename = "type", default)]
    pub source_type: SourceKind,

    /// Remote host for SSH connections (e.g., "user@laptop.local").
    #[serde(default)]
    pub host: Option<String>,

    /// Paths to sync from this source.
    /// For SSH sources, these are remote paths.
    /// Supports ~ expansion.
    #[serde(default)]
    pub paths: Vec<String>,

    /// When to automatically sync this source.
    #[serde(default)]
    pub sync_schedule: SyncSchedule,

    /// Path mappings for workspace rewriting.
    /// Maps remote paths to local equivalents.
    /// Example: "/home/user/projects" -> "/Users/me/projects"
    #[serde(default)]
    pub path_mappings: HashMap<String, String>,

    /// Platform hint for default paths (macos, linux).
    #[serde(default)]
    pub platform: Option<Platform>,
}

impl SourceDefinition {
    /// Create a new local source definition.
    pub fn local(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source_type: SourceKind::Local,
            ..Default::default()
        }
    }

    /// Create a new SSH source definition.
    pub fn ssh(name: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source_type: SourceKind::Ssh,
            host: Some(host.into()),
            ..Default::default()
        }
    }

    /// Check if this source requires SSH connectivity.
    pub fn is_remote(&self) -> bool {
        matches!(self.source_type, SourceKind::Ssh)
    }

    /// Validate the source definition.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::Validation(
                "Source name cannot be empty".into(),
            ));
        }

        if self.name.contains('/') || self.name.contains('\\') {
            return Err(ConfigError::Validation(
                "Source name cannot contain path separators".into(),
            ));
        }

        if self.is_remote() && self.host.is_none() {
            return Err(ConfigError::Validation("SSH sources require a host".into()));
        }

        Ok(())
    }

    /// Apply path mapping to rewrite a workspace path.
    /// Uses longest-prefix matching.
    pub fn rewrite_path(&self, path: &str) -> String {
        // Sort by prefix length descending for longest-prefix match
        let mut mappings: Vec<_> = self.path_mappings.iter().collect();
        mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

        for (from, to) in mappings {
            if path.starts_with(from) {
                return path.replacen(from, to, 1);
            }
        }

        path.to_string()
    }
}

/// Sync schedule for remote sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyncSchedule {
    /// Only sync when explicitly requested.
    #[default]
    Manual,
    /// Sync every hour.
    Hourly,
    /// Sync once per day.
    Daily,
}

impl std::fmt::Display for SyncSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => write!(f, "manual"),
            Self::Hourly => write!(f, "hourly"),
            Self::Daily => write!(f, "daily"),
        }
    }
}

/// Platform hint for choosing default paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Macos,
    Linux,
    Windows,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Macos => write!(f, "macos"),
            Self::Linux => write!(f, "linux"),
            Self::Windows => write!(f, "windows"),
        }
    }
}

impl SourcesConfig {
    /// Load configuration from the default location.
    ///
    /// Returns an empty config if the file doesn't exist.
    pub fn load() -> Result<Self, ConfigError> {
        let config_path = Self::config_path()?;

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(&config_path)?;
        let config: Self = toml::from_str(&content)?;

        // Validate all sources
        config.validate()?;

        Ok(config)
    }

    /// Load configuration from a specific path.
    pub fn load_from(path: &PathBuf) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;

        Ok(config)
    }

    /// Save configuration to the default location.
    pub fn save(&self) -> Result<(), ConfigError> {
        let config_path = Self::config_path()?;

        // Create parent directories if needed
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content = toml::to_string_pretty(self)?;
        std::fs::write(&config_path, content)?;

        Ok(())
    }

    /// Save configuration to a specific path.
    pub fn save_to(&self, path: &PathBuf) -> Result<(), ConfigError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;

        Ok(())
    }

    /// Get the default configuration file path.
    ///
    /// Uses XDG conventions:
    /// - Primary: `$XDG_CONFIG_HOME/cass/sources.toml`
    /// - Fallback: platform-specific config dir (e.g., `~/.config/cass/sources.toml` on Linux)
    pub fn config_path() -> Result<PathBuf, ConfigError> {
        // Respect XDG_CONFIG_HOME first (important for testing and Linux users)
        if let Ok(xdg_config) = std::env::var("XDG_CONFIG_HOME") {
            return Ok(PathBuf::from(xdg_config).join("cass").join("sources.toml"));
        }

        dirs::config_dir()
            .map(|p| p.join("cass").join("sources.toml"))
            .ok_or(ConfigError::NoConfigDir)
    }

    /// Validate all sources in the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Check for duplicate names
        let mut seen_names = std::collections::HashSet::new();
        for source in &self.sources {
            source.validate()?;

            if !seen_names.insert(&source.name) {
                return Err(ConfigError::Validation(format!(
                    "Duplicate source name: {}",
                    source.name
                )));
            }
        }

        Ok(())
    }

    /// Find a source by name.
    pub fn find_source(&self, name: &str) -> Option<&SourceDefinition> {
        self.sources.iter().find(|s| s.name == name)
    }

    /// Find a source by name (mutable).
    pub fn find_source_mut(&mut self, name: &str) -> Option<&mut SourceDefinition> {
        self.sources.iter_mut().find(|s| s.name == name)
    }

    /// Add a new source. Returns error if name already exists.
    pub fn add_source(&mut self, source: SourceDefinition) -> Result<(), ConfigError> {
        source.validate()?;

        if self.sources.iter().any(|s| s.name == source.name) {
            return Err(ConfigError::Validation(format!(
                "Source '{}' already exists",
                source.name
            )));
        }

        self.sources.push(source);
        Ok(())
    }

    /// Remove a source by name. Returns true if found and removed.
    pub fn remove_source(&mut self, name: &str) -> bool {
        let initial_len = self.sources.len();
        self.sources.retain(|s| s.name != name);
        self.sources.len() < initial_len
    }

    /// Get all remote sources (SSH type).
    pub fn remote_sources(&self) -> impl Iterator<Item = &SourceDefinition> {
        self.sources.iter().filter(|s| s.is_remote())
    }
}

/// Get preset paths for a given platform.
///
/// These are the default agent session directories for each platform.
pub fn get_preset_paths(preset: &str) -> Result<Vec<String>, ConfigError> {
    match preset {
        "macos-defaults" | "macos" => Ok(vec![
            "~/.claude/projects".into(),
            "~/.codex/sessions".into(),
            "~/Library/Application Support/Cursor/User/globalStorage/rooveterinaryinc.roo-cline/settings".into(),
            "~/Library/Application Support/com.openai.chat".into(),
            "~/.gemini/tmp".into(),
            "~/.pi/agent/sessions".into(),
            "~/.local/share/opencode".into(),
            "~/.continue/sessions".into(),
            "~/.aider.chat.history.md".into(),
            "~/.goose/sessions".into(),
        ]),
        "linux-defaults" | "linux" => Ok(vec![
            "~/.claude/projects".into(),
            "~/.codex/sessions".into(),
            "~/.config/Cursor/User/globalStorage/rooveterinaryinc.roo-cline/settings".into(),
            "~/.gemini/tmp".into(),
            "~/.pi/agent/sessions".into(),
            "~/.local/share/opencode".into(),
            "~/.continue/sessions".into(),
            "~/.aider.chat.history.md".into(),
            "~/.goose/sessions".into(),
        ]),
        _ => Err(ConfigError::Validation(format!(
            "Unknown preset: '{}'. Valid presets: macos-defaults, linux-defaults",
            preset
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_config_default() {
        let config = SourcesConfig::default();
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_source_definition_local() {
        let source = SourceDefinition::local("test");
        assert_eq!(source.name, "test");
        assert_eq!(source.source_type, SourceKind::Local);
        assert!(!source.is_remote());
    }

    #[test]
    fn test_source_definition_ssh() {
        let source = SourceDefinition::ssh("laptop", "user@laptop.local");
        assert_eq!(source.name, "laptop");
        assert_eq!(source.source_type, SourceKind::Ssh);
        assert_eq!(source.host, Some("user@laptop.local".into()));
        assert!(source.is_remote());
    }

    #[test]
    fn test_source_validation_empty_name() {
        let source = SourceDefinition::default();
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_ssh_without_host() {
        let mut source = SourceDefinition::ssh("test", "host");
        source.host = None;
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_path_rewriting() {
        let mut source = SourceDefinition::local("test");
        source
            .path_mappings
            .insert("/home/user/projects".into(), "/Users/me/projects".into());
        source
            .path_mappings
            .insert("/home/user".into(), "/Users/me".into());

        // Longest prefix should match
        assert_eq!(
            source.rewrite_path("/home/user/projects/myapp"),
            "/Users/me/projects/myapp"
        );

        // Shorter prefix
        assert_eq!(source.rewrite_path("/home/user/other"), "/Users/me/other");

        // No match
        assert_eq!(source.rewrite_path("/opt/data"), "/opt/data");
    }

    #[test]
    fn test_config_duplicate_names() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition::local("test"));
        config.sources.push(SourceDefinition::local("test"));

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_add_source() {
        let mut config = SourcesConfig::default();
        config.add_source(SourceDefinition::local("test")).unwrap();

        assert_eq!(config.sources.len(), 1);

        // Adding duplicate should fail
        assert!(config.add_source(SourceDefinition::local("test")).is_err());
    }

    #[test]
    fn test_config_remove_source() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition::local("test"));

        assert!(config.remove_source("test"));
        assert!(!config.remove_source("nonexistent"));
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition {
            name: "laptop".into(),
            source_type: SourceKind::Ssh,
            host: Some("user@laptop.local".into()),
            paths: vec!["~/.claude/projects".into()],
            sync_schedule: SyncSchedule::Daily,
            path_mappings: {
                let mut m = HashMap::new();
                m.insert("/home/user".into(), "/Users/me".into());
                m
            },
            platform: Some(Platform::Linux),
        });

        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: SourcesConfig = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.sources.len(), 1);
        assert_eq!(deserialized.sources[0].name, "laptop");
        assert_eq!(deserialized.sources[0].sync_schedule, SyncSchedule::Daily);
    }

    #[test]
    fn test_preset_paths() {
        let macos = get_preset_paths("macos-defaults").unwrap();
        assert!(!macos.is_empty());
        assert!(macos.iter().any(|p| p.contains(".claude")));

        let linux = get_preset_paths("linux-defaults").unwrap();
        assert!(!linux.is_empty());

        assert!(get_preset_paths("unknown").is_err());
    }

    #[test]
    fn test_sync_schedule_display() {
        assert_eq!(SyncSchedule::Manual.to_string(), "manual");
        assert_eq!(SyncSchedule::Hourly.to_string(), "hourly");
        assert_eq!(SyncSchedule::Daily.to_string(), "daily");
    }
}
