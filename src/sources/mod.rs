//! Remote sources management for cass.
//!
//! This module provides functionality for configuring and syncing agent session
//! data from remote machines via SSH. It enables cass to search across conversation
//! history from multiple machines.
//!
//! # Architecture
//!
//! - **config**: Configuration types for defining remote sources
//! - **provenance**: Types for tracking conversation origins
//! - **sync** (future): Sync engine for pulling sessions from remotes
//! - **status** (future): Sync status tracking
//!
//! # Configuration
//!
//! Sources are configured in `~/.config/cass/sources.toml`:
//!
//! ```toml
//! [[sources]]
//! name = "laptop"
//! type = "ssh"
//! host = "user@laptop.local"
//! paths = ["~/.claude/projects", "~/.cursor"]
//! ```
//!
//! # Provenance
//!
//! Each conversation tracks where it came from via [`provenance::Origin`]:
//!
//! ```rust,ignore
//! use coding_agent_search::sources::provenance::{Origin, SourceKind};
//!
//! // Local conversation
//! let local = Origin::local();
//!
//! // Remote conversation
//! let remote = Origin::remote("work-laptop");
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use coding_agent_search::sources::config::SourcesConfig;
//!
//! // Load configuration
//! let config = SourcesConfig::load()?;
//!
//! // Iterate remote sources
//! for source in config.remote_sources() {
//!     println!("Source: {} ({})", source.name, source.host.as_deref().unwrap_or("-"));
//! }
//! ```

pub mod config;
pub mod provenance;

// Re-export commonly used config types
pub use config::{
    ConfigError, Platform, SourceDefinition, SourcesConfig, SyncSchedule, get_preset_paths,
};

// Re-export commonly used provenance types
pub use provenance::{LOCAL_SOURCE_ID, Origin, Source, SourceFilter, SourceKind};
