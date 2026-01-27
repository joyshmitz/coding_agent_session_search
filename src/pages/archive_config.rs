//! Archive configuration types for pages bundles.
//!
//! Supports both encrypted and unencrypted bundles via an untagged enum.

use serde::{Deserialize, Serialize};

use super::encrypt::EncryptionConfig;

/// Supported archive configuration formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ArchiveConfig {
    /// Encrypted bundle configuration (default).
    Encrypted(EncryptionConfig),
    /// Unencrypted bundle configuration.
    Unencrypted(UnencryptedConfig),
}

impl ArchiveConfig {
    /// Returns true if this config represents an encrypted bundle.
    pub fn is_encrypted(&self) -> bool {
        matches!(self, ArchiveConfig::Encrypted(_))
    }

    /// Get the encrypted config if available.
    pub fn as_encrypted(&self) -> Option<&EncryptionConfig> {
        match self {
            ArchiveConfig::Encrypted(cfg) => Some(cfg),
            ArchiveConfig::Unencrypted(_) => None,
        }
    }

    /// Get the unencrypted config if available.
    pub fn as_unencrypted(&self) -> Option<&UnencryptedConfig> {
        match self {
            ArchiveConfig::Encrypted(_) => None,
            ArchiveConfig::Unencrypted(cfg) => Some(cfg),
        }
    }
}

/// Unencrypted bundle configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnencryptedConfig {
    /// Whether the bundle is encrypted (must be false).
    pub encrypted: bool,
    /// Config version.
    pub version: String,
    /// Payload descriptor.
    pub payload: UnencryptedPayload,
    /// Optional warning message for viewers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Unencrypted payload descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnencryptedPayload {
    /// Relative path to the SQLite database payload.
    pub path: String,
    /// Payload format (e.g., "sqlite").
    pub format: String,
    /// Optional byte size of the payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}
