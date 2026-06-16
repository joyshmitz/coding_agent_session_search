//! Provider-specific connector-ingest diagnostic taxonomy.
//!
//! Bead: coding_agent_session_search-cass-fleet-resilience-20260608-uojcg.15.1
//! ("Harden connector ingest diagnostics and per-provider failure fixtures").
//!
//! Connectors are the front door for 20 local agents, so a connector failure
//! becomes lost, merged, duplicated, or misleading search history. The report
//! named concrete cases: an Aider external-id collision that merges distinct
//! project histories, malformed JSONL that could panic/abort a whole file, a
//! Cursor `state.vscdb` locked by the live app, ChatGPT encrypted stores that
//! are undecipherable on Linux/headless hosts, and Amp filename-prefix
//! assumptions that break on non-ASCII / future schemes.
//!
//! The failure these share is *silence*: a provider-specific limitation gets
//! turned into a fake zero-message import, or two unrelated conversations get
//! merged under one id. This module makes the failure **explicit,
//! provider-specific, retry-aware, and testable**: an [`IngestFailureKind`]
//! classifies what went wrong, [`classify`] turns it into a
//! [`ConnectorIngestDiagnostic`] carrying provider/source/provenance/line/id
//! context plus severity, retryability, the per-source
//! [`SourceIngestDisposition`], and a safe next action, and
//! [`ProviderIngestSummary`] distinguishes discovered / skipped / quarantined /
//! partially-indexed / locked / unsupported-encrypted / successfully-indexed
//! sources instead of collapsing them into one count.
//!
//! It also carries the one concrete behavioural fix that is pure logic:
//! [`disambiguated_external_id`] folds workspace/source identity into a
//! connector external id so two distinct project histories can never collide
//! and merge.
//!
//! This is pure, side-effect-free classification — the connectors (here and in
//! `franken_agent_detection`) produce the facts; this turns them into a stable,
//! serializable diagnostic. Wiring the taxonomy into the live ingest summary and
//! the per-provider connector hardening land in a follow-on.

use serde::{Deserialize, Serialize};

/// Stable schema version for the connector-ingest diagnostic wire format.
pub const CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION: u32 = 1;

/// How serious a single ingest finding is. Ordered low→high so a per-source or
/// per-run rollup can take the worst with `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestSeverity {
    /// Informational; ingestion proceeded normally.
    Info,
    /// A limitation or a quarantined record; the rest of the source still
    /// indexed. Not a hard failure.
    Warning,
    /// The source could not be ingested at all (and it is not merely an absent
    /// or encrypted-but-known limitation).
    Error,
}

impl IngestSeverity {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestSeverity::Info => "info",
            IngestSeverity::Warning => "warning",
            IngestSeverity::Error => "error",
        }
    }
}

/// What specifically went wrong while ingesting a connector source. Each kind is
/// the report's concrete connector failure mode, generalised across providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestFailureKind {
    /// A connector external id would collide across distinct workspaces/sources
    /// and merge unrelated histories (the Aider case) unless disambiguated.
    ExternalIdCollisionRisk,
    /// A single JSONL line/record was malformed; valid records around it are
    /// preserved and this one is quarantined (never a whole-file abort/panic).
    MalformedJsonLine,
    /// A session file was truncated mid-stream; records up to the truncation are
    /// preserved.
    TruncatedSession,
    /// The source is locked by the live app (Cursor `state.vscdb`); retryable.
    SourceLocked,
    /// The source is encrypted and cannot be decrypted on this host
    /// (ChatGPT v2/v3 on Linux/headless); legacy unencrypted data still indexes.
    EncryptedUnsupported,
    /// A required keychain/credential to decrypt is unavailable (headless host).
    KeychainUnavailable,
    /// A filename/path assumption did not hold (Amp prefix scheme, non-ASCII, or
    /// a future-looking name); the source is skipped rather than mis-parsed.
    FilenameAssumptionViolated,
    /// The source path exists but could not be read (permissions / I/O).
    UnreadableSource,
}

impl IngestFailureKind {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestFailureKind::ExternalIdCollisionRisk => "external-id-collision-risk",
            IngestFailureKind::MalformedJsonLine => "malformed-json-line",
            IngestFailureKind::TruncatedSession => "truncated-session",
            IngestFailureKind::SourceLocked => "source-locked",
            IngestFailureKind::EncryptedUnsupported => "encrypted-unsupported",
            IngestFailureKind::KeychainUnavailable => "keychain-unavailable",
            IngestFailureKind::FilenameAssumptionViolated => "filename-assumption-violated",
            IngestFailureKind::UnreadableSource => "unreadable-source",
        }
    }
}

/// The per-source outcome an ingest run reports, so a limitation is never
/// silently rendered as "indexed 0 messages". Ordered roughly best→worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceIngestDisposition {
    /// Fully ingested; every record indexed.
    Indexed,
    /// Discovered but not yet processed (enumerated, pending).
    Discovered,
    /// Some records indexed, others quarantined/truncated.
    PartiallyIndexed,
    /// One or more records quarantined; the source itself is otherwise indexed.
    Quarantined,
    /// Skipped (filename/path assumption violated, or deliberately excluded).
    Skipped,
    /// Locked by the live app; retry later (no data indexed this run).
    Locked,
    /// Encrypted and unsupported on this host (a known, explicit limitation —
    /// never a fake zero-message success).
    UnsupportedEncrypted,
}

impl SourceIngestDisposition {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            SourceIngestDisposition::Indexed => "indexed",
            SourceIngestDisposition::Discovered => "discovered",
            SourceIngestDisposition::PartiallyIndexed => "partially-indexed",
            SourceIngestDisposition::Quarantined => "quarantined",
            SourceIngestDisposition::Skipped => "skipped",
            SourceIngestDisposition::Locked => "locked",
            SourceIngestDisposition::UnsupportedEncrypted => "unsupported-encrypted",
        }
    }

    /// Whether this disposition reflects an explicit, non-silent limitation
    /// (locked / unsupported-encrypted / skipped) — the thing that must never be
    /// reported as a successful zero-message import.
    pub const fn is_explicit_limitation(self) -> bool {
        matches!(
            self,
            SourceIngestDisposition::Locked
                | SourceIngestDisposition::UnsupportedEncrypted
                | SourceIngestDisposition::Skipped
        )
    }
}

/// A single connector-ingest diagnostic. Stable snake_case JSON with an embedded
/// [`schema_version`](Self::schema_version).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorIngestDiagnostic {
    /// Mirrors [`CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// The provider/connector (e.g. `"aider"`, `"cursor"`, `"chatgpt"`).
    pub provider: String,
    /// The source path or root the finding came from.
    pub source_path: String,
    /// Workspace / source provenance identity, when known (distinguishes the
    /// same base id across projects/machines).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// Line/row identifier when the finding is record-scoped (e.g. a malformed
    /// JSONL line number).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_or_row: Option<u64>,
    /// The connector external id in context, when relevant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    /// The canonical (post-disambiguation) id, when computed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
    /// What went wrong.
    pub failure_kind: IngestFailureKind,
    /// How serious it is.
    pub severity: IngestSeverity,
    /// Whether retrying later may succeed (e.g. a transient lock).
    pub retryable: bool,
    /// The per-source disposition this finding implies.
    pub disposition: SourceIngestDisposition,
    /// A single, safe next action for an operator/agent. Always read-only or a
    /// safe re-run; never a destructive cleanup.
    pub safe_next_action: String,
}

/// The classification of a failure kind for a provider: severity, retryability,
/// the disposition it implies, and the safe next action. Pure.
fn classify_kind(
    provider: &str,
    kind: IngestFailureKind,
) -> (IngestSeverity, bool, SourceIngestDisposition, String) {
    match kind {
        IngestFailureKind::ExternalIdCollisionRisk => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::Indexed,
            format!(
                "{provider} external ids are disambiguated by workspace/source identity so distinct \
                 project histories cannot merge; verify the canonical id includes provenance"
            ),
        ),
        IngestFailureKind::MalformedJsonLine => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::PartiallyIndexed,
            "the malformed line is quarantined and the surrounding valid records are indexed; \
             inspect the quarantined line — the whole file is never aborted"
                .to_string(),
        ),
        IngestFailureKind::TruncatedSession => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::PartiallyIndexed,
            "the session was truncated; records up to the truncation are indexed — re-export the \
             session if the tail is needed"
                .to_string(),
        ),
        IngestFailureKind::SourceLocked => (
            IngestSeverity::Warning,
            true, // retryable: the lock is transient
            SourceIngestDisposition::Locked,
            format!(
                "{provider} source is locked by the live app (e.g. an open Cursor); this is \
                 reported as lock-busy, NOT a zero-message import — retry after closing the app or \
                 on the next sync"
            ),
        ),
        IngestFailureKind::EncryptedUnsupported => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::UnsupportedEncrypted,
            format!(
                "{provider} store is encrypted and unsupported on this host; legacy unencrypted \
                 conversations still index — this is an explicit limitation, never a fake success"
            ),
        ),
        IngestFailureKind::KeychainUnavailable => (
            IngestSeverity::Warning,
            true, // a keychain may become available on a non-headless run
            SourceIngestDisposition::UnsupportedEncrypted,
            format!(
                "{provider} decryption needs a keychain/credential unavailable on this headless \
                 host; index from an interactive session where the keychain is unlocked"
            ),
        ),
        IngestFailureKind::FilenameAssumptionViolated => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::Skipped,
            format!(
                "{provider} filename did not match the expected scheme (non-ASCII or a future \
                 naming variant); skipped rather than mis-parsed — file a fixture for the variant"
            ),
        ),
        IngestFailureKind::UnreadableSource => (
            IngestSeverity::Error,
            true, // permissions/IO may be fixed and retried
            SourceIngestDisposition::Skipped,
            format!("{provider} source path could not be read; check permissions, then re-index"),
        ),
    }
}

/// Build a full [`ConnectorIngestDiagnostic`] for a provider failure. Pure.
pub fn classify(
    provider: &str,
    source_path: &str,
    kind: IngestFailureKind,
) -> ConnectorIngestDiagnostic {
    let (severity, retryable, disposition, safe_next_action) = classify_kind(provider, kind);
    ConnectorIngestDiagnostic {
        schema_version: CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION,
        provider: provider.to_string(),
        source_path: source_path.to_string(),
        workspace: None,
        line_or_row: None,
        external_id: None,
        canonical_id: None,
        failure_kind: kind,
        severity,
        retryable,
        disposition,
        safe_next_action,
    }
}

impl ConnectorIngestDiagnostic {
    /// Attach workspace/source provenance.
    pub fn with_workspace(mut self, workspace: impl Into<String>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    /// Attach a record line/row identifier.
    pub fn with_line(mut self, line: u64) -> Self {
        self.line_or_row = Some(line);
        self
    }

    /// Attach the external id and its disambiguated canonical id.
    pub fn with_ids(
        mut self,
        external_id: impl Into<String>,
        canonical_id: impl Into<String>,
    ) -> Self {
        self.external_id = Some(external_id.into());
        self.canonical_id = Some(canonical_id.into());
        self
    }
}

/// Fold workspace/source identity into a connector external id so that two
/// distinct project histories sharing the same base id can never collide and
/// merge (the Aider external-id collision). Deterministic: the same
/// `(base_id, workspace)` always yields the same canonical id, and different
/// workspaces always diverge. An empty workspace returns the base id unchanged
/// (nothing to disambiguate by).
pub fn disambiguated_external_id(base_id: &str, workspace: &str) -> String {
    if workspace.is_empty() {
        return base_id.to_string();
    }
    let digest = blake3::hash(workspace.as_bytes());
    let short: String = digest.to_hex().to_string().chars().take(12).collect();
    format!("{base_id}@{short}")
}

/// A per-provider rollup of source dispositions for a run. Replaces a single
/// "indexed N" count with the full breakdown so a locked/encrypted source is
/// never invisible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProviderIngestSummary {
    pub discovered: u64,
    pub indexed: u64,
    pub partially_indexed: u64,
    pub quarantined: u64,
    pub skipped: u64,
    pub locked: u64,
    pub unsupported_encrypted: u64,
}

impl ProviderIngestSummary {
    /// Record one source's disposition.
    pub fn record(&mut self, disposition: SourceIngestDisposition) {
        match disposition {
            SourceIngestDisposition::Discovered => self.discovered += 1,
            SourceIngestDisposition::Indexed => self.indexed += 1,
            SourceIngestDisposition::PartiallyIndexed => self.partially_indexed += 1,
            SourceIngestDisposition::Quarantined => self.quarantined += 1,
            SourceIngestDisposition::Skipped => self.skipped += 1,
            SourceIngestDisposition::Locked => self.locked += 1,
            SourceIngestDisposition::UnsupportedEncrypted => self.unsupported_encrypted += 1,
        }
    }

    /// Total sources accounted for.
    pub fn total(&self) -> u64 {
        self.discovered
            + self.indexed
            + self.partially_indexed
            + self.quarantined
            + self.skipped
            + self.locked
            + self.unsupported_encrypted
    }

    /// Sources that produced at least some searchable content (indexed or
    /// partially indexed). Used to assert a run is not a silent total miss.
    pub fn with_content(&self) -> u64 {
        self.indexed + self.partially_indexed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- per-provider classification --------------------------------------

    #[test]
    fn cursor_lock_is_retryable_lock_busy_not_a_zero_import() {
        let d = classify(
            "cursor",
            "~/Library/.../state.vscdb",
            IngestFailureKind::SourceLocked,
        );
        assert_eq!(d.disposition, SourceIngestDisposition::Locked);
        assert!(d.retryable, "a live-app lock is transient and retryable");
        assert!(d.disposition.is_explicit_limitation());
        assert!(
            d.safe_next_action.contains("NOT a zero-message import"),
            "a lock must never read as a successful empty import: {}",
            d.safe_next_action
        );
    }

    #[test]
    fn chatgpt_encrypted_is_unsupported_not_silent_and_legacy_still_indexes() {
        let d = classify(
            "chatgpt",
            "~/.../com.openai.chat",
            IngestFailureKind::EncryptedUnsupported,
        );
        assert_eq!(d.disposition, SourceIngestDisposition::UnsupportedEncrypted);
        assert!(!d.retryable);
        assert!(d.disposition.is_explicit_limitation());
        assert!(d.safe_next_action.contains("legacy unencrypted"));
        assert!(d.safe_next_action.contains("never a fake success"));
    }

    #[test]
    fn keychain_unavailable_is_retryable_from_interactive_session() {
        let d = classify(
            "chatgpt",
            "~/.../com.openai.chat",
            IngestFailureKind::KeychainUnavailable,
        );
        assert_eq!(d.disposition, SourceIngestDisposition::UnsupportedEncrypted);
        assert!(d.retryable);
        assert!(d.safe_next_action.contains("headless"));
    }

    #[test]
    fn malformed_json_line_quarantines_the_line_and_preserves_the_rest() {
        let d = classify(
            "codex",
            "rollout-x.jsonl",
            IngestFailureKind::MalformedJsonLine,
        )
        .with_line(42);
        assert_eq!(d.disposition, SourceIngestDisposition::PartiallyIndexed);
        assert_eq!(d.line_or_row, Some(42));
        assert!(
            d.safe_next_action.contains("never aborted"),
            "a malformed line must not abort the whole file"
        );
        assert_eq!(d.severity, IngestSeverity::Warning);
    }

    #[test]
    fn truncated_session_indexes_up_to_truncation() {
        let d = classify(
            "claude_code",
            "session.jsonl",
            IngestFailureKind::TruncatedSession,
        );
        assert_eq!(d.disposition, SourceIngestDisposition::PartiallyIndexed);
    }

    #[test]
    fn amp_filename_assumption_violation_is_skipped_not_misparsed() {
        let d = classify(
            "amp",
            "weird-名前.json",
            IngestFailureKind::FilenameAssumptionViolated,
        );
        assert_eq!(d.disposition, SourceIngestDisposition::Skipped);
        assert!(d.disposition.is_explicit_limitation());
        assert!(d.safe_next_action.contains("non-ASCII"));
    }

    #[test]
    fn unreadable_source_is_an_error_but_retryable() {
        let d = classify(
            "gemini",
            "~/.gemini/tmp",
            IngestFailureKind::UnreadableSource,
        );
        assert_eq!(d.severity, IngestSeverity::Error);
        assert_eq!(d.disposition, SourceIngestDisposition::Skipped);
        assert!(d.retryable);
    }

    #[test]
    fn every_failure_kind_has_a_nonempty_safe_action_and_no_destructive_op() {
        let kinds = [
            IngestFailureKind::ExternalIdCollisionRisk,
            IngestFailureKind::MalformedJsonLine,
            IngestFailureKind::TruncatedSession,
            IngestFailureKind::SourceLocked,
            IngestFailureKind::EncryptedUnsupported,
            IngestFailureKind::KeychainUnavailable,
            IngestFailureKind::FilenameAssumptionViolated,
            IngestFailureKind::UnreadableSource,
        ];
        let destructive = ["rm ", "--delete", "--purge", "reset", "drop ", "rm -rf"];
        for kind in kinds {
            let d = classify("aider", "/some/path", kind);
            assert!(
                !d.safe_next_action.is_empty(),
                "{} has no action",
                kind.as_str()
            );
            let lower = d.safe_next_action.to_ascii_lowercase();
            for bad in destructive {
                assert!(
                    !lower.contains(bad),
                    "{} safe action references a destructive op {bad:?}: {}",
                    kind.as_str(),
                    d.safe_next_action
                );
            }
        }
    }

    // --- Aider external-id disambiguation ---------------------------------

    #[test]
    fn distinct_workspaces_never_collide_on_the_same_base_id() {
        let a = disambiguated_external_id("aider-history", "/home/me/project-a");
        let b = disambiguated_external_id("aider-history", "/home/me/project-b");
        assert_ne!(
            a, b,
            "distinct project workspaces must not merge under one id"
        );
        assert!(a.starts_with("aider-history@"));
    }

    #[test]
    fn same_workspace_and_base_id_is_stable() {
        let a = disambiguated_external_id("aider-history", "/home/me/project-a");
        let b = disambiguated_external_id("aider-history", "/home/me/project-a");
        assert_eq!(a, b, "disambiguation must be deterministic");
    }

    #[test]
    fn empty_workspace_returns_base_id_unchanged() {
        assert_eq!(disambiguated_external_id("base", ""), "base");
    }

    #[test]
    fn diagnostic_can_carry_disambiguated_ids() {
        let canonical = disambiguated_external_id("h", "/ws/proj");
        let d = classify(
            "aider",
            "/ws/proj/.aider.chat.history.md",
            IngestFailureKind::ExternalIdCollisionRisk,
        )
        .with_workspace("/ws/proj")
        .with_ids("h", canonical.clone());
        assert_eq!(d.external_id.as_deref(), Some("h"));
        assert_eq!(d.canonical_id.as_deref(), Some(canonical.as_str()));
        assert_eq!(d.workspace.as_deref(), Some("/ws/proj"));
    }

    // --- provider summary -------------------------------------------------

    #[test]
    fn summary_distinguishes_every_disposition() {
        let mut s = ProviderIngestSummary::default();
        for disp in [
            SourceIngestDisposition::Discovered,
            SourceIngestDisposition::Indexed,
            SourceIngestDisposition::Indexed,
            SourceIngestDisposition::PartiallyIndexed,
            SourceIngestDisposition::Quarantined,
            SourceIngestDisposition::Skipped,
            SourceIngestDisposition::Locked,
            SourceIngestDisposition::UnsupportedEncrypted,
        ] {
            s.record(disp);
        }
        assert_eq!(s.discovered, 1);
        assert_eq!(s.indexed, 2);
        assert_eq!(s.partially_indexed, 1);
        assert_eq!(s.quarantined, 1);
        assert_eq!(s.skipped, 1);
        assert_eq!(s.locked, 1);
        assert_eq!(s.unsupported_encrypted, 1);
        assert_eq!(s.total(), 8);
        assert_eq!(s.with_content(), 3); // indexed + partially-indexed
    }

    #[test]
    fn a_locked_or_encrypted_only_run_has_no_content_but_is_not_silent() {
        // The anti-pattern guard: a run where every source is locked/encrypted
        // must report zero content AND surface the explicit limitations.
        let mut s = ProviderIngestSummary::default();
        s.record(SourceIngestDisposition::Locked);
        s.record(SourceIngestDisposition::UnsupportedEncrypted);
        assert_eq!(s.with_content(), 0, "no searchable content");
        assert!(
            s.total() > 0,
            "but the sources are accounted for, not silently dropped"
        );
        assert!(SourceIngestDisposition::Locked.is_explicit_limitation());
        assert!(SourceIngestDisposition::UnsupportedEncrypted.is_explicit_limitation());
    }

    // --- serialization stability ------------------------------------------

    #[test]
    fn diagnostic_serializes_with_stable_fields_and_round_trips() {
        let d = classify("cursor", "/p/state.vscdb", IngestFailureKind::SourceLocked)
            .with_workspace("/p");
        let value = serde_json::to_value(&d).expect("to_value");
        assert_eq!(
            value["schema_version"],
            CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION
        );
        assert_eq!(value["provider"], "cursor");
        assert_eq!(value["failure_kind"], "source-locked");
        assert_eq!(value["severity"], "warning");
        assert_eq!(value["disposition"], "locked");
        assert_eq!(value["retryable"], true);
        let back: ConnectorIngestDiagnostic = serde_json::from_value(value).expect("round-trip");
        assert_eq!(back, d);
    }

    #[test]
    fn summary_serializes_with_stable_snake_case_keys() {
        let mut s = ProviderIngestSummary::default();
        s.record(SourceIngestDisposition::UnsupportedEncrypted);
        let value = serde_json::to_value(s).expect("to_value");
        assert_eq!(value["unsupported_encrypted"], 1);
        assert_eq!(value["indexed"], 0);
    }

    #[test]
    fn wire_labels_are_stable_kebab() {
        for (k, w) in [
            (
                IngestFailureKind::ExternalIdCollisionRisk,
                "external-id-collision-risk",
            ),
            (IngestFailureKind::MalformedJsonLine, "malformed-json-line"),
            (IngestFailureKind::SourceLocked, "source-locked"),
            (
                IngestFailureKind::EncryptedUnsupported,
                "encrypted-unsupported",
            ),
            (
                IngestFailureKind::KeychainUnavailable,
                "keychain-unavailable",
            ),
            (
                IngestFailureKind::FilenameAssumptionViolated,
                "filename-assumption-violated",
            ),
            (IngestFailureKind::UnreadableSource, "unreadable-source"),
            (IngestFailureKind::TruncatedSession, "truncated-session"),
        ] {
            assert_eq!(serde_json::to_string(&k).expect("ser"), format!("\"{w}\""));
            assert_eq!(k.as_str(), w);
        }
        for (d, w) in [
            (SourceIngestDisposition::Indexed, "indexed"),
            (
                SourceIngestDisposition::PartiallyIndexed,
                "partially-indexed",
            ),
            (
                SourceIngestDisposition::UnsupportedEncrypted,
                "unsupported-encrypted",
            ),
            (SourceIngestDisposition::Locked, "locked"),
        ] {
            assert_eq!(serde_json::to_string(&d).expect("ser"), format!("\"{w}\""));
            assert_eq!(d.as_str(), w);
        }
        assert_eq!(IngestSeverity::Warning.as_str(), "warning");
    }

    #[test]
    fn severity_orders_for_a_max_rollup() {
        assert!(IngestSeverity::Info < IngestSeverity::Warning);
        assert!(IngestSeverity::Warning < IngestSeverity::Error);
        let worst = [
            IngestSeverity::Info,
            IngestSeverity::Error,
            IngestSeverity::Warning,
        ]
        .into_iter()
        .max()
        .expect("non-empty");
        assert_eq!(worst, IngestSeverity::Error);
    }
}
