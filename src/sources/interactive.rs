//! Interactive terminal prompts for the remote sources setup wizard.
//!
//! This module provides rich interactive components using dialoguer, including:
//! - Multi-select host picker with multi-line item display
//! - Confirmation prompts for destructive operations
//! - Progress display integration with indicatif
//!
//! # Design Decision: dialoguer vs inquire
//!
//! We chose dialoguer because:
//! 1. It integrates well with indicatif (already used for progress bars)
//! 2. It's actively maintained and widely used
//! 3. It supports ANSI styling in items via the console crate
//!
//! # Multi-line Item Display
//!
//! Standard dialoguer MultiSelect shows single-line items. We achieve multi-line
//! display by embedding ANSI escape sequences and newlines directly in item strings:
//!
//! ```text
//! [x] css
//!     209.145.54.164 • ubuntu
//!     ✓ cass v0.1.50 installed • 1,234 sessions
//!     Claude ✓  Codex ✓  Cursor ✓
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use coding_agent_search::sources::interactive::{HostSelector, HostDisplayInfo, CassStatusDisplay};
//!
//! let hosts = vec![
//!     HostDisplayInfo {
//!         name: "css".into(),
//!         hostname: "209.145.54.164".into(),
//!         username: "ubuntu".into(),
//!         cass_status: CassStatusDisplay::Installed { version: "0.1.50".into(), sessions: 1234 },
//!         detected_agents: vec!["claude".into(), "codex".into()],
//!         reachable: true,
//!         error: None,
//!     },
//!     // ... more hosts
//! ];
//!
//! let selector = HostSelector::new(hosts);
//! let selected = selector.prompt()?;
//! ```

use std::fmt;

use colored::Colorize;
use dialoguer::{Confirm, MultiSelect, theme::ColorfulTheme};

// =============================================================================
// Types
// =============================================================================

/// Display information for a remote host in the selection UI.
#[derive(Debug, Clone)]
pub struct HostDisplayInfo {
    /// SSH config name (e.g., "css", "laptop")
    pub name: String,
    /// IP address or hostname
    pub hostname: String,
    /// SSH username
    pub username: String,
    /// cass installation status on this host
    pub cass_status: CassStatusDisplay,
    /// Detected coding agents on this host
    pub detected_agents: Vec<String>,
    /// Whether this host is reachable
    pub reachable: bool,
    /// Optional error message if unreachable
    pub error: Option<String>,
}

/// cass installation status for display purposes.
#[derive(Debug, Clone)]
pub enum CassStatusDisplay {
    /// cass is installed with known version and session count
    Installed { version: String, sessions: u64 },
    /// cass is not installed but agent data was detected
    NotInstalled,
    /// Could not determine status (e.g., probe failed)
    Unknown,
}

/// Result of host selection.
#[derive(Debug, Clone)]
pub struct HostSelectionResult {
    /// Indices of selected hosts
    pub selected_indices: Vec<usize>,
    /// Hosts that need cass installation
    pub needs_install: Vec<usize>,
    /// Hosts ready for sync
    pub ready_for_sync: Vec<usize>,
}

// =============================================================================
// Host Selector
// =============================================================================

/// Interactive multi-select host picker with rich display.
pub struct HostSelector {
    hosts: Vec<HostDisplayInfo>,
    theme: ColorfulTheme,
}

impl HostSelector {
    /// Create a new host selector with the given hosts.
    pub fn new(hosts: Vec<HostDisplayInfo>) -> Self {
        Self {
            hosts,
            theme: ColorfulTheme::default(),
        }
    }

    /// Format a single host for multi-line display.
    ///
    /// Returns a string with ANSI formatting suitable for terminal display.
    fn format_host(&self, host: &HostDisplayInfo) -> String {
        let mut lines = Vec::new();

        // Line 1: Host name (bold)
        let name_line = host.name.bold().to_string();
        lines.push(name_line);

        // Line 2: Hostname and username (dimmed)
        let host_info = format!(
            "    {} • {}",
            host.hostname.dimmed(),
            host.username.dimmed()
        );
        lines.push(host_info);

        // Line 3: cass status
        let status_line = match &host.cass_status {
            CassStatusDisplay::Installed { version, sessions } => {
                format!(
                    "    {} cass v{} • {} sessions",
                    "✓".green(),
                    version,
                    sessions
                )
            }
            CassStatusDisplay::NotInstalled => {
                format!("    {} cass not installed", "✗".yellow())
            }
            CassStatusDisplay::Unknown => {
                format!("    {} status unknown", "?".dimmed())
            }
        };
        lines.push(status_line);

        // Line 4: Detected agents (if any)
        if !host.detected_agents.is_empty() {
            let agents: Vec<String> = host
                .detected_agents
                .iter()
                .map(|a| format!("{} {}", a.cyan(), "✓".green()))
                .collect();
            let agents_line = format!("    {}", agents.join("  "));
            lines.push(agents_line);
        }

        // Line 5: Error if unreachable
        if !host.reachable {
            let error_msg = host.error.as_deref().unwrap_or("unreachable");
            let error_line = format!("    {} {}", "⚠".red(), error_msg.red());
            lines.push(error_line);
        }

        lines.join("\n")
    }

    /// Show the interactive multi-select prompt.
    ///
    /// Returns the selection result or an error if the prompt was cancelled.
    pub fn prompt(&self) -> Result<HostSelectionResult, InteractiveError> {
        if self.hosts.is_empty() {
            return Err(InteractiveError::NoHosts);
        }

        // Format all hosts for display
        let items: Vec<String> = self.hosts.iter().map(|h| self.format_host(h)).collect();

        // Pre-select reachable hosts with cass installed
        let defaults: Vec<bool> = self
            .hosts
            .iter()
            .map(|h| h.reachable && matches!(h.cass_status, CassStatusDisplay::Installed { .. }))
            .collect();

        // Show the prompt
        println!();
        println!(
            "{}",
            "Select hosts to configure as sources:".bold().underline()
        );
        println!(
            "{}",
            "[space] toggle  [a] all  [enter] confirm  [q] quit".dimmed()
        );
        println!();

        let selected = MultiSelect::with_theme(&self.theme)
            .items(&items)
            .defaults(&defaults)
            .interact_opt()
            .map_err(|e| InteractiveError::IoError(e.to_string()))?
            .ok_or(InteractiveError::Cancelled)?;

        // Categorize selections
        let mut needs_install = Vec::new();
        let mut ready_for_sync = Vec::new();

        for &idx in &selected {
            if idx < self.hosts.len() {
                let host = &self.hosts[idx];
                if host.reachable {
                    match host.cass_status {
                        CassStatusDisplay::Installed { .. } => ready_for_sync.push(idx),
                        CassStatusDisplay::NotInstalled | CassStatusDisplay::Unknown => {
                            needs_install.push(idx)
                        }
                    }
                }
            }
        }

        Ok(HostSelectionResult {
            selected_indices: selected,
            needs_install,
            ready_for_sync,
        })
    }

    /// Get host info by index.
    pub fn get_host(&self, index: usize) -> Option<&HostDisplayInfo> {
        self.hosts.get(index)
    }
}

// =============================================================================
// Confirmation Prompts
// =============================================================================

/// Ask for confirmation before a destructive operation.
pub fn confirm_action(message: &str, default: bool) -> Result<bool, InteractiveError> {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(default)
        .interact()
        .map_err(|e| InteractiveError::IoError(e.to_string()))
}

/// Ask for confirmation with a detailed explanation.
pub fn confirm_with_details(
    action: &str,
    details: &[&str],
    default: bool,
) -> Result<bool, InteractiveError> {
    println!();
    println!("{}", action.bold());
    for detail in details {
        println!("  • {}", detail);
    }
    println!();

    confirm_action("Proceed?", default)
}

// =============================================================================
// Errors
// =============================================================================

/// Errors from interactive prompts.
#[derive(Debug)]
pub enum InteractiveError {
    /// User cancelled the prompt
    Cancelled,
    /// No hosts available to select
    NoHosts,
    /// IO error during prompt
    IoError(String),
}

impl fmt::Display for InteractiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InteractiveError::Cancelled => write!(f, "Operation cancelled by user"),
            InteractiveError::NoHosts => write!(f, "No hosts available for selection"),
            InteractiveError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for InteractiveError {}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_display_info_creation() {
        let host = HostDisplayInfo {
            name: "laptop".into(),
            hostname: "192.168.1.100".into(),
            username: "user".into(),
            cass_status: CassStatusDisplay::Installed {
                version: "0.1.50".into(),
                sessions: 123,
            },
            detected_agents: vec!["claude".into(), "codex".into()],
            reachable: true,
            error: None,
        };

        assert_eq!(host.name, "laptop");
        assert!(host.reachable);
        assert!(matches!(
            host.cass_status,
            CassStatusDisplay::Installed { .. }
        ));
    }

    #[test]
    fn test_host_selector_format() {
        let hosts = vec![HostDisplayInfo {
            name: "test-host".into(),
            hostname: "10.0.0.1".into(),
            username: "testuser".into(),
            cass_status: CassStatusDisplay::NotInstalled,
            detected_agents: vec!["claude".into()],
            reachable: true,
            error: None,
        }];

        let selector = HostSelector::new(hosts);
        let formatted = selector.format_host(&selector.hosts[0]);

        // Check that formatting includes expected content
        assert!(formatted.contains("test-host"));
        assert!(formatted.contains("10.0.0.1"));
        assert!(formatted.contains("testuser"));
        assert!(formatted.contains("cass not installed"));
        assert!(formatted.contains("claude"));
    }

    #[test]
    fn test_host_selector_empty() {
        let selector = HostSelector::new(vec![]);
        // Can't actually call prompt() in tests, but we can verify error handling
        assert!(selector.hosts.is_empty());
    }

    #[test]
    fn test_cass_status_display_variants() {
        let installed = CassStatusDisplay::Installed {
            version: "0.1.50".into(),
            sessions: 100,
        };
        let not_installed = CassStatusDisplay::NotInstalled;
        let unknown = CassStatusDisplay::Unknown;

        assert!(matches!(installed, CassStatusDisplay::Installed { .. }));
        assert!(matches!(not_installed, CassStatusDisplay::NotInstalled));
        assert!(matches!(unknown, CassStatusDisplay::Unknown));
    }

    #[test]
    fn test_host_selection_result() {
        let result = HostSelectionResult {
            selected_indices: vec![0, 2, 3],
            needs_install: vec![2],
            ready_for_sync: vec![0, 3],
        };

        assert_eq!(result.selected_indices.len(), 3);
        assert_eq!(result.needs_install.len(), 1);
        assert_eq!(result.ready_for_sync.len(), 2);
    }

    #[test]
    fn test_interactive_error_display() {
        let cancelled = InteractiveError::Cancelled;
        let no_hosts = InteractiveError::NoHosts;
        let io_error = InteractiveError::IoError("test error".into());

        assert!(cancelled.to_string().contains("cancelled"));
        assert!(no_hosts.to_string().contains("No hosts"));
        assert!(io_error.to_string().contains("test error"));
    }

    #[test]
    fn test_unreachable_host_format() {
        let hosts = vec![HostDisplayInfo {
            name: "unreachable-host".into(),
            hostname: "10.0.0.99".into(),
            username: "user".into(),
            cass_status: CassStatusDisplay::Unknown,
            detected_agents: vec![],
            reachable: false,
            error: Some("Connection timed out".into()),
        }];

        let selector = HostSelector::new(hosts);
        let formatted = selector.format_host(&selector.hosts[0]);

        assert!(formatted.contains("unreachable-host"));
        assert!(formatted.contains("Connection timed out"));
    }
}
