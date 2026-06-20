//! Metadata-only trust/provenance scoring for search hits, sessions, and answer
//! pack evidence.
//!
//! Bead: coding_agent_session_search-guided-ops-repro-trust-5u82n.3
//! ("Trust-score search results and answer packs with provenance signals").
//!
//! ## Why
//!
//! cass surfaces *relevant* old conversations, but relevance is not correctness.
//! A result can be a landed, release-contained fix — or a failed agent attempt,
//! a superseded workaround, or advice from a different workspace. This module
//! reduces a small set of **metadata-only** signals into a compact verdict
//! ([`TrustAssessment`]) so robot consumers can tell "reuse this" from
//! "do not copy this" without re-reading the conversation.
//!
//! ## Metadata-only, no raw text (by construction)
//!
//! [`TrustSignals`] carries only derived signals — ages, booleans, an outcome
//! marker, and structured identifiers (commit sha, bead id, release tag). There
//! is no field that holds raw session/prompt/tool text, so this layer cannot
//! leak it. Provenance refs are compact, sanitized identifiers
//! ([`sanitize_ref_value`]); commit shas are truncated. The verdict never
//! changes result ordering — it is advisory metadata only.
//!
//! ## Pure and deterministic
//!
//! [`assess_trust`] does no I/O and is a pure function of its input, so the same
//! signals always yield the same verdict, refs, reason, and confidence — safe to
//! pin in golden tests and fixtures.

use serde::{Deserialize, Serialize};

/// Stable schema version for the trust wire format.
pub const TRUST_SCHEMA_VERSION: u32 = 1;

/// A coarse, ordered verdict on how much to trust a result. Declaration order is
/// most-trusted → least-trusted, so derived `Ord` ranks `Trusted < … < Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustTier {
    /// Landed, proof-backed work with strong provenance (release-contained
    /// commit and/or a closed bead with proof). Safe to reuse.
    Trusted,
    /// Probably current — has provenance (a linked commit or closed bead) but is
    /// not fully proof-backed/release-pinned. Confirm before relying on it.
    Likely,
    /// Relevant but unverified: no provenance links, an open bead, or the result
    /// could only be corroborated lexically.
    Unverified,
    /// Known to be out of date — superseded by newer work or aged past its
    /// useful window with no release containment.
    Stale,
    /// A failed or reverted attempt. Do not reuse.
    Failed,
}

impl TrustTier {
    /// Stable snake_case wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            TrustTier::Trusted => "trusted",
            TrustTier::Likely => "likely",
            TrustTier::Unverified => "unverified",
            TrustTier::Stale => "stale",
            TrustTier::Failed => "failed",
        }
    }
}

/// Confidence in the trust verdict itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustConfidence {
    Low,
    Medium,
    High,
}

impl TrustConfidence {
    /// Stable snake_case wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            TrustConfidence::Low => "low",
            TrustConfidence::Medium => "medium",
            TrustConfidence::High => "high",
        }
    }

    /// The lower (more cautious) of two confidences.
    fn floor(self, other: TrustConfidence) -> TrustConfidence {
        if self <= other { self } else { other }
    }
}

/// The recorded outcome of the work a result represents (a metadata marker
/// derived from linked commits/beads and known stale/failed signals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeMarker {
    /// Linked to a landed commit or a closed bead.
    Landed,
    /// A failed command / reverted attempt.
    Failed,
    /// A newer result supersedes this one.
    Superseded,
    /// Linked to an open / in-progress bead.
    Open,
    /// No outcome signal available.
    Unknown,
}

/// Proof status of the linked work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProofStatus {
    /// Linked bead/commit carries proof artifacts (tests/gates/E2E).
    Proven,
    /// Claimed done but with outstanding proof debt.
    ProofDebt,
    /// No proof signal.
    Unknown,
}

/// Which search tier actually realized the result (lexical-only means semantic
/// could not corroborate the match).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealizedMode {
    Hybrid,
    Semantic,
    Lexical,
}

/// Metadata-only inputs to trust scoring. Carries no raw session text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustSignals {
    /// Age of the result in days (None when unknown).
    #[serde(default)]
    pub age_days: Option<u64>,
    /// Whether the result's workspace matches the active query workspace.
    pub workspace_match: bool,
    /// Linked commit sha, if any (provenance; truncated into refs).
    #[serde(default)]
    pub linked_commit: Option<String>,
    /// Linked closed bead id, if any.
    #[serde(default)]
    pub linked_closed_bead: Option<String>,
    /// Recorded outcome marker.
    pub outcome: OutcomeMarker,
    /// Proof status of the linked work.
    pub proof: ProofStatus,
    /// Release tag that contains the linked commit, if known.
    #[serde(default)]
    pub release_tag: Option<String>,
    /// Whether the backing source is healthy/reachable.
    pub source_healthy: bool,
    /// Realized search tier.
    pub realized_mode: RealizedMode,
}

impl Default for TrustSignals {
    fn default() -> Self {
        TrustSignals {
            age_days: None,
            workspace_match: true,
            linked_commit: None,
            linked_closed_bead: None,
            outcome: OutcomeMarker::Unknown,
            proof: ProofStatus::Unknown,
            release_tag: None,
            source_healthy: true,
            realized_mode: RealizedMode::Hybrid,
        }
    }
}

/// The metadata-only trust verdict exposed in robot output. Advisory; it never
/// changes result ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustAssessment {
    /// Mirrors [`TRUST_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Coarse trust tier.
    pub trust_tier: TrustTier,
    /// Confidence in the verdict.
    pub confidence: TrustConfidence,
    /// Sanitized provenance identifiers (e.g. `commit:ab0d12ef90ab`, `bead:xyz`,
    /// `release:v0.6.15`). Deterministic order; never raw text or paths.
    pub provenance_refs: Vec<String>,
    /// Stable snake_case code for the dominant reason trust is reduced, when the
    /// result is not fully trusted (e.g. `failed_attempt`, `superseded_by_newer`,
    /// `linked_bead_open`, `aged_out`, `workspace_mismatch`, `source_unhealthy`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    /// Advisory next step (text only — never a destructive command).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_followup: Option<String>,
}

/// Age past which a result with no release containment is treated as stale.
const STALE_AGE_DAYS: u64 = 365;
/// Age past which an already-unverified result is flagged as aging.
const AGING_AGE_DAYS: u64 = 180;
/// Max characters kept from a commit sha in a provenance ref.
const COMMIT_REF_LEN: usize = 12;

/// Keep only characters safe for a structured identifier ref (alphanumerics and
/// id punctuation). Drops whitespace, path separators, quotes, and any other
/// character, so a ref cannot smuggle raw text, paths, or an injection phrase.
fn sanitize_ref_value(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        .take(64)
        .collect()
}

/// Build the deterministic, sanitized provenance ref list.
fn build_provenance_refs(signals: &TrustSignals) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(commit) = &signals.linked_commit {
        let short: String = commit
            .chars()
            .filter(|c| c.is_ascii_hexdigit())
            .take(COMMIT_REF_LEN)
            .collect();
        if !short.is_empty() {
            refs.push(format!("commit:{short}"));
        }
    }
    if let Some(bead) = &signals.linked_closed_bead {
        let id = sanitize_ref_value(bead);
        if !id.is_empty() {
            refs.push(format!("bead:{id}"));
        }
    }
    if let Some(tag) = &signals.release_tag {
        let tag = sanitize_ref_value(tag);
        if !tag.is_empty() {
            refs.push(format!("release:{tag}"));
        }
    }
    refs
}

/// Score a result's trust from metadata-only [`TrustSignals`]. Pure and
/// deterministic.
pub fn assess_trust(signals: &TrustSignals) -> TrustAssessment {
    let provenance_refs = build_provenance_refs(signals);
    let has_commit = signals.linked_commit.is_some();
    let has_bead = signals.linked_closed_bead.is_some();
    let has_release = signals.release_tag.is_some();
    let proven = matches!(signals.proof, ProofStatus::Proven);
    let has_provenance = has_commit || has_bead;

    // 1) Negative outcomes dominate — never reuse a failed or superseded result.
    if matches!(signals.outcome, OutcomeMarker::Failed) {
        return TrustAssessment {
            schema_version: TRUST_SCHEMA_VERSION,
            trust_tier: TrustTier::Failed,
            confidence: TrustConfidence::Low,
            provenance_refs,
            stale_reason: Some("failed_attempt".to_string()),
            recommended_followup: Some(
                "This was a failed or reverted attempt — prefer a newer landed result.".to_string(),
            ),
        };
    }
    if matches!(signals.outcome, OutcomeMarker::Superseded) {
        return TrustAssessment {
            schema_version: TRUST_SCHEMA_VERSION,
            trust_tier: TrustTier::Stale,
            confidence: TrustConfidence::Low,
            provenance_refs,
            stale_reason: Some("superseded_by_newer".to_string()),
            recommended_followup: Some(
                "A newer change supersedes this — look for the superseding fix.".to_string(),
            ),
        };
    }

    // 2) Positive provenance strength sets the base tier.
    let landed = matches!(signals.outcome, OutcomeMarker::Landed);
    let mut tier = if landed && proven && (has_release || has_bead) {
        TrustTier::Trusted
    } else if has_provenance {
        TrustTier::Likely
    } else {
        TrustTier::Unverified
    };

    // 3) Demotions + dominant reason (first/highest-priority reason wins) +
    //    confidence floor. The check order below is the reason priority order.
    let mut confidence = TrustConfidence::High;
    let mut reason: Option<&'static str> = None;

    // Source health gates how sure we are the result is reachable/actionable,
    // not whether the underlying work landed — it lowers confidence only.
    if !signals.source_healthy {
        confidence = confidence.floor(TrustConfidence::Low);
        reason.get_or_insert("source_unhealthy");
    }
    if matches!(signals.outcome, OutcomeMarker::Open) {
        if tier < TrustTier::Unverified {
            tier = TrustTier::Unverified;
        }
        confidence = confidence.floor(TrustConfidence::Medium);
        reason.get_or_insert("linked_bead_open");
    }
    if let Some(age) = signals.age_days {
        if age >= STALE_AGE_DAYS && !has_release {
            if tier < TrustTier::Stale {
                tier = TrustTier::Stale;
            }
            confidence = confidence.floor(TrustConfidence::Medium);
            reason.get_or_insert("aged_out");
        } else if age >= AGING_AGE_DAYS && matches!(tier, TrustTier::Unverified) {
            confidence = confidence.floor(TrustConfidence::Medium);
            reason.get_or_insert("aged_out");
        }
    }
    if !signals.workspace_match {
        confidence = confidence.floor(TrustConfidence::Medium);
        reason.get_or_insert("workspace_mismatch");
    }
    // Lexical-only realization means semantic could not corroborate the match.
    if matches!(signals.realized_mode, RealizedMode::Lexical) {
        confidence = confidence.floor(TrustConfidence::Medium);
    }
    // No provenance at all caps confidence regardless of relevance.
    if !has_provenance {
        confidence = confidence.floor(TrustConfidence::Medium);
    }
    // An unverified tier is never high-confidence.
    if matches!(tier, TrustTier::Unverified) {
        confidence = confidence.floor(TrustConfidence::Medium);
    }

    let recommended_followup = match tier {
        TrustTier::Trusted => None,
        TrustTier::Likely => Some(
            "Likely current — confirm via the cited commit or closed bead before relying on it."
                .to_string(),
        ),
        TrustTier::Unverified => Some(
            "Unverified — corroborate before reuse (no linked commit or closed bead).".to_string(),
        ),
        TrustTier::Stale => Some(
            "Aged out of its useful window — prefer a newer, release-contained result.".to_string(),
        ),
        // Failed/Superseded already returned above.
        TrustTier::Failed => None,
    };

    TrustAssessment {
        schema_version: TRUST_SCHEMA_VERSION,
        trust_tier: tier,
        confidence,
        provenance_refs,
        stale_reason: reason.map(str::to_string),
        recommended_followup,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn landed_release_proven() -> TrustSignals {
        TrustSignals {
            age_days: Some(10),
            workspace_match: true,
            linked_commit: Some("ab0d12ef90abcdef1234567890abcdef12345678".to_string()),
            linked_closed_bead: Some("xyz123".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::Proven,
            release_tag: Some("v0.6.15".to_string()),
            source_healthy: true,
            realized_mode: RealizedMode::Hybrid,
        }
    }

    // ---- the seven required fixture cases ----------------------------------

    #[test]
    fn landed_commit_is_trusted_high() {
        let a = assess_trust(&landed_release_proven());
        assert_eq!(a.trust_tier, TrustTier::Trusted);
        assert_eq!(a.confidence, TrustConfidence::High);
        assert_eq!(a.stale_reason, None);
        assert!(
            a.provenance_refs
                .contains(&"commit:ab0d12ef90ab".to_string())
        );
        assert!(a.provenance_refs.contains(&"release:v0.6.15".to_string()));
        assert!(a.provenance_refs.contains(&"bead:xyz123".to_string()));
    }

    #[test]
    fn closed_bead_with_proof_is_trusted() {
        // Proven, landed, closed bead — but no commit/release.
        let s = TrustSignals {
            linked_commit: None,
            linked_closed_bead: Some("bead-9".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::Proven,
            release_tag: None,
            age_days: Some(20),
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Trusted);
        assert_eq!(a.confidence, TrustConfidence::High);
        assert_eq!(a.provenance_refs, vec!["bead:bead-9".to_string()]);
    }

    #[test]
    fn open_bead_is_unverified_with_reason() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Open,
            linked_closed_bead: None,
            proof: ProofStatus::ProofDebt,
            age_days: Some(5),
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Unverified);
        assert_eq!(a.stale_reason.as_deref(), Some("linked_bead_open"));
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn failed_command_is_failed_low() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Failed,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Failed);
        assert_eq!(a.confidence, TrustConfidence::Low);
        assert_eq!(a.stale_reason.as_deref(), Some("failed_attempt"));
        assert!(a.recommended_followup.is_some());
    }

    #[test]
    fn superseded_fix_is_stale() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Superseded,
            linked_commit: Some("deadbeefdeadbeefdeadbeef".to_string()),
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Stale);
        assert_eq!(a.stale_reason.as_deref(), Some("superseded_by_newer"));
        // Provenance still surfaced even when stale.
        assert!(a.provenance_refs.iter().any(|r| r.starts_with("commit:")));
    }

    #[test]
    fn lexical_only_fallback_caps_confidence() {
        // Otherwise-trusted result, but only lexical corroboration.
        let mut s = landed_release_proven();
        s.realized_mode = RealizedMode::Lexical;
        let a = assess_trust(&s);
        // Tier unchanged (ordering not affected), but confidence is capped.
        assert_eq!(a.trust_tier, TrustTier::Trusted);
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn workspace_mismatch_lowers_confidence_and_sets_reason() {
        let mut s = landed_release_proven();
        s.workspace_match = false;
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Trusted, "ordering/tier unaffected");
        assert!(a.confidence <= TrustConfidence::Medium);
        assert_eq!(a.stale_reason.as_deref(), Some("workspace_mismatch"));
    }

    // ---- additional invariants ---------------------------------------------

    #[test]
    fn aged_unreleased_result_becomes_stale() {
        let s = TrustSignals {
            age_days: Some(400),
            linked_commit: Some("abcabcabcabc".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::ProofDebt,
            release_tag: None,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Stale);
        assert_eq!(a.stale_reason.as_deref(), Some("aged_out"));
    }

    #[test]
    fn no_provenance_is_unverified_capped_medium() {
        let a = assess_trust(&TrustSignals::default());
        assert_eq!(a.trust_tier, TrustTier::Unverified);
        assert!(a.provenance_refs.is_empty());
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn source_unhealthy_dominates_reason_and_drops_confidence() {
        let mut s = landed_release_proven();
        s.source_healthy = false;
        let a = assess_trust(&s);
        assert_eq!(a.confidence, TrustConfidence::Low);
        assert_eq!(a.stale_reason.as_deref(), Some("source_unhealthy"));
    }

    #[test]
    fn refs_are_sanitized_to_safe_tokens() {
        // Pathological ids: spaces, path separators, quotes, an injection phrase.
        let s = TrustSignals {
            linked_closed_bead: Some("bd-1 /home/alice/x 'or' 1=1".to_string()),
            release_tag: Some("v1.0 (drop table users);".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::Proven,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        for r in &a.provenance_refs {
            // Each ref is `prefix:value`; the value carries only id chars.
            let value = r.split_once(':').map(|(_, v)| v).unwrap_or(r);
            assert!(
                value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')),
                "ref value not token-safe: {r}"
            );
        }
        let joined = a.provenance_refs.join(" ");
        assert!(
            !joined.contains('/'),
            "no path separators in refs: {joined}"
        );
        assert!(
            !joined.contains("table users"),
            "injection phrase broken: {joined}"
        );
    }

    #[test]
    fn assessment_json_contract_is_stable_and_round_trips() {
        let a = assess_trust(&landed_release_proven());
        let value = serde_json::to_value(&a).unwrap();
        assert_eq!(value["schema_version"], TRUST_SCHEMA_VERSION);
        assert_eq!(value["trust_tier"], "trusted");
        assert_eq!(value["confidence"], "high");
        // stale_reason omitted (None) when fully trusted.
        assert!(value.get("stale_reason").is_none());
        let back: TrustAssessment = serde_json::from_value(value).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn scoring_is_deterministic() {
        let s = landed_release_proven();
        assert_eq!(assess_trust(&s), assess_trust(&s));
    }

    #[test]
    fn tier_and_confidence_wire_labels_are_stable() {
        assert_eq!(TrustTier::Trusted.as_str(), "trusted");
        assert_eq!(TrustTier::Failed.as_str(), "failed");
        assert_eq!(TrustConfidence::High.as_str(), "high");
        // Ordering: more trusted sorts lower; higher confidence sorts higher.
        assert!(TrustTier::Trusted < TrustTier::Failed);
        assert!(TrustConfidence::Low < TrustConfidence::High);
    }
}
