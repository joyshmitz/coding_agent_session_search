# No-Mock Audit Report

Generated: 2026-01-27 (post vhl0 real-model refactor)

## Executive Summary

This audit catalogs remaining mock/fake/stub patterns in the cass codebase.

**Status:** ✅ Mock implementations removed from search/embedder/reranker/daemon tests.

**Current allowlist:** 2 entries (true fixture boundaries only)
- Deterministic fixture constructors: 2 entries (`mock_system_info`, `mock_resources`)

**Matches found:** 29 (all in `src/sources/install.rs`)

**CI validation:** `./scripts/validate_ci.sh --no-mock-only` should pass with the updated allowlist.

## Classification Categories

- **(a) REMOVE/REPLACE**: Mock that should be replaced with real implementation
- **(b) CONVERT TO FIXTURE**: Mock data that should use real recorded sessions/data
- **(c) ALLOWLIST**: True platform boundary or deterministic fixture constructor

---

## Source Code (`src/`)

### 1. `src/sources/install.rs`

**Classification: (c) ALLOWLIST - Deterministic fixture constructors**

Patterns:
- `mock_system_info`
- `mock_resources`

**Decision:** These helpers construct `SystemInfo` / `ResourceInfo` for pure
function unit tests (install method selection and resource checks). They are
non-network, deterministic fixtures and are complemented by real system probe
integration tests.

**Review date:** 2026-07-27

---

## Test Files (`tests/`)

No remaining mock/fake/stub patterns in tests outside of fixture directories
and documentation comments.

---

## Change Log

- 2026-01-27: Removed MockEmbedder/MockReranker/MockDaemon tests in favor of
  real FastEmbed model fixtures (see vhl0). Allowlist reduced to install
  fixture constructors only.
