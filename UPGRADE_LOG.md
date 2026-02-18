# Dependency Upgrade Log

**Date:** 2026-02-17  
**Project:** coding_agent_session_search (`cass`)  
**Language:** Rust

## Summary
- **Updated:** 3 direct dependency lines in `Cargo.toml` (`reqwest`, `rand`, `rand_chacha`)
- **Migrated code:** rand 0.10 API updates across runtime/test/bench callsites
- **Validated:** `cargo check --all-targets`, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`
- **Remaining behind latest:** 3 transitive crates (`generic-array`, `hnsw_rs`, `libc`)

## Direct Dependency Updates

### reqwest: 0.12.28 -> 0.13.2
- **Manifest change:** `features = ["json", "rustls-tls", "blocking", "multipart"]` -> `features = ["json", "rustls", "blocking", "multipart"]`
- **Reason:** reqwest 0.13 removed `rustls-tls` feature name
- **Status:** ✅ Compiles and passes strict clippy

### rand: 0.8.5 -> 0.10.0
- **Manifest change:** `rand = "0.8"` -> `rand = "0.10"`
- **Code migration:** replaced old APIs (`thread_rng`, `gen`, `gen_range`) with rand 0.10 APIs (`rng`, `random`, `random_range`) and updated RNG callsites used by export/encryption helpers
- **Status:** ✅ Compiles and passes strict clippy

### rand_chacha: 0.3.1 -> 0.10.0
- **Manifest change:** dev dependency `rand_chacha = "0.3"` -> `rand_chacha = "0.10"`
- **Code migration:** updated deterministic test RNG usage in `tests/util/mod.rs`
- **Status:** ✅ Compiles and passes strict clippy

## Cargo Resolution Notes
- `cargo update --verbose` now reports only these unresolved transitive updates:
  - `generic-array v0.14.7` (available `0.14.9`)
  - `hnsw_rs v0.3.2` (available `0.3.3`)
  - `libc v0.2.180` (available `0.2.182`)

## Validation Run
- `cargo check --all-targets` ✅
- `cargo fmt --check` ✅
- `cargo clippy --all-targets -- -D warnings` ✅

## Files Touched for rand/reqwest Migration
- `Cargo.toml`
- `Cargo.lock`
- `src/lib.rs`
- `src/pages/encrypt.rs`
- `src/pages/key_management.rs`
- `src/pages/qr.rs`
- `src/pages/wizard.rs`
- `src/html_export/encryption.rs`
- `tests/util/mod.rs`
- `benches/crypto_perf.rs`
- `benches/export_perf.rs`
