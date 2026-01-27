# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.63] - 2026-01-27

### Added

#### Approximate Nearest Neighbor (HNSW) Search
- **HNSW Index**: New Hierarchical Navigable Small World graph for O(log n) semantic search, dramatically improving query latency on large indexes
- **CLI Flags**: `--build-hnsw` flag for `cass index` to build the ANN index, `--approximate` flag for `cass search` to use HNSW instead of linear scan
- **ANN Statistics**: Search results now include timing breakdowns and ANN-specific metrics in robot mode output
- **Configurable Parameters**: M=16, ef_construction=200, ef_search=100 for ~95-99% recall with sub-millisecond latency

#### HTML Session Export
- **`export-html` Command**: New CLI command to export conversations as beautiful, self-contained HTML files
- **Password Encryption**: Optional AES-256-GCM encryption with Argon2id key derivation (600,000 iterations) for secure sharing
- **TUI Export Modal**: Press `e` in detail view or `Ctrl+E` for quick export with encryption options
- **Multi-Agent Support**: Export sessions from any supported agent (Claude, Codex, Cursor, etc.) with proper formatting
- **Rich Rendering**: Syntax-highlighted code blocks, collapsible tool calls, print-friendly layouts, dark/light themes
- **Smart Filenames**: Auto-generated descriptive filenames based on session metadata and timestamps

#### Encrypted GitHub Pages Web Export
- **Pages Bundle System**: Complete encrypted static site export for hosting on GitHub Pages or any static host
- **Browser Decryption**: Client-side AES-256-GCM decryption using Web Crypto API with PBKDF2 key derivation
- **Service Worker**: Offline-first architecture with COOP/COEP headers for cross-origin isolation
- **FTS5 Search**: Full-text search in browser via sqlite-wasm, searchable even when hosted statically
- **Deployment Wizard**: Interactive TUI wizard for generating and deploying encrypted bundles
- **Cloudflare Integration**: Direct deployment to Cloudflare Pages with automatic configuration
- **Preview Server**: Local preview server for testing bundles before deployment
- **Attachment Support**: Bundles can include conversation attachments with integrity verification
- **Secret Scanning**: Pre-publish scanner detects API keys, tokens, and sensitive data before bundling
- **Unencrypted Option**: Support for non-encrypted bundles with explicit risk acknowledgment

#### Multi-Machine Remote Sources
- **`cass sources setup` Wizard**: Interactive wizard for configuring multi-machine search
- **SSH Host Discovery**: Automatically discovers hosts from `~/.ssh/config` with filtering
- **Host Probing**: Checks each host for cass installation, agent data, system resources
- **Remote Installation**: Installs cass on remote machines via cargo-binstall, pre-built binaries, or full bootstrap
- **Sync Engine**: rsync-based delta transfers with automatic SFTP fallback, additive-only (no `--delete`)
- **Path Mappings**: Workspace path rewriting for consistent cross-machine references
- **Provenance Tracking**: Source ID, origin kind, and origin host fields track where each conversation came from
- **Resumable Setup**: Interrupted wizard sessions can be resumed with `--resume` flag

#### Comprehensive Test Infrastructure
- **PhaseTracker**: Centralized test phase tracking with Drop-based auto-completion for E2E tests
- **JSONL Structured Logging**: Standardized logging format with phase markers, timestamps, and trace IDs
- **E2E Logging Compliance**: CI check validates all E2E tests use standard logging infrastructure
- **Real Fixture Policy**: No-mock testing with real session data, ONNX models, and connector fixtures
- **Fixture Factory**: Modular fixture loading with provenance hashes and MANIFEST.json documentation
- **Connector Edge-Case Tests**: Comprehensive robustness tests for all 11 connectors (Aider, Amp, ChatGPT, Claude, Cline, Codex, Cursor, Gemini, OpenCode, PiAgent, Factory)
- **Playwright Browser E2E**: Cross-browser testing (Chromium, Firefox, WebKit) for HTML exports
- **SSH E2E Tests**: Real SSH-based integration tests with Docker containers

#### Security Hardening
- **Path Traversal Protection**: Comprehensive detection of Unicode normalization attacks, RTL override characters, zero-width characters, and homoglyph confusables
- **XSS Prevention**: FTS5 snippet HTML sanitization prevents stored XSS in search results
- **URL Encoding Bypass Tests**: Validation against double-encoding and mixed-encoding attacks
- **Secret Detection**: Pre-publish scanner with configurable patterns and redaction

#### Query Parser Enhancements
- **Nested Sub-Terms**: ParsedTerm restructured to support recursive term nesting for complex queries
- **Boolean Operators**: Full support for AND/OR/NOT operators with proper precedence
- **Stress Tests**: Comprehensive query parser stress testing for edge cases and malformed input
- **Improved Wildcards**: Better handling of prefix, suffix, and infix wildcards

#### New Agent Connectors
- **Factory (Droid)**: Full support for Factory AI's Droid coding agent (`~/.factory/sessions/`)
- **Pi-Agent Enhancements**: Extended thinking content extraction and model change tracking

### Changed

#### Performance Improvements
- **Robot Field Filtering**: Optimized `--fields minimal` preset for 30-50% faster robot mode responses
- **OpenCode Connector**: Per-message directory loading reduces memory usage for large sessions
- **Index Reader Reload**: Force initial reload on Manual policy prevents stale results
- **Legacy Path Fallback**: XDG migration preserves access to pre-migration state files

#### CLI & Robot Mode
- **TOON Output Format**: Token-efficient output format (`format='toon'`) for AI agent communication
- **Timing Breakdown**: Robot output includes `open_ms`, `query_ms`, and phase-specific timings
- **Structured Index Stats**: `cass index --json` returns detailed indexing statistics (T7.4)
- **CLI Aliases**: `--robot` and `--force` shorthand aliases for common flags

#### TUI Improvements
- **Score Indicator Widget**: Extracted to reusable component with consistent styling
- **Contextual Snippet Optimization**: Faster snippet generation for search results
- **Export Modal Integration**: Seamless export workflow from detail view

### Fixed

#### Critical Fixes
- **Search Index Reload**: Fixed stale results when using Manual reload policy
- **Source Path Mapping**: Preserve path separators in directory mappings for cross-platform sync
- **Remote Installer Alignment**: Match GitHub release asset naming conventions
- **Tilde Expansion Guard**: Early return when remote home directory is unavailable
- **Bloom Filter Flakiness**: Fixed non-deterministic bloom filter assertion in tests

#### Connector Fixes
- **Gemini Path Detection**: Simplified path end detection logic
- **OpenCode Lints**: Fixed clippy warnings and use Path over PathBuf in signatures
- **ChatGPT Robustness**: Edge-case handling for malformed session files

#### Security Fixes
- **FTS5 Snippet XSS**: Sanitize HTML in search result snippets
- **Encoded Path Checks**: Hardened URL encoding validation in path verification

### Removed

- **Fake Binary Allowlist**: E2E tests now use real binaries exclusively
- **Mock Types in Tests**: Replaced MockHit/MockPane with real types

---

## [0.1.57] - 2026-01-19

### Added

#### Semantic Search Infrastructure
- **Embedder Registry**: Model selection system for choosing between embedding backends
- **Daemon Client**: Warm embedder/reranker via background daemon for faster repeated queries
- **Reranker Support**: Cross-encoder reranking for improved result quality
- **Model Management**: Automatic model download with retry logic and verification

#### Storage Improvements
- **Incremental Commits**: Streaming indexer commits changes during ingest for crash recovery
- **SQLite ID Caching**: Cached lookups reduce database round-trips during indexing
- **Batched Stats Updates**: Efficient daily_stats updates prevent double-counting

### Fixed

- **Stats Source Filter**: Correct SQL for source-filtered statistics queries
- **Rsync Path Handling**: Explicit UTF-8 error handling instead of unwrap
- **TUI Digit Parse**: Safe parsing prevents panic on malformed input
- **Connector Hardening**: Robust parsing for edge cases across all connectors

---

## [0.1.56] - 2026-01-15

### Added

#### Pages Export Foundation
- **Bundle Verification**: CI/CD command for validating encrypted bundles
- **Pre-Publish Summary**: Generate human-readable summary before publishing
- **Share Profiles**: Privacy presets for different sharing scenarios (public, team, private)
- **Package Manager Notifications**: ACFS workflow for installer change notifications

### Fixed

- **rusqlite 0.38 Compatibility**: Resolved type inference errors with new rusqlite version
- **Migration Safety**: PRAGMA foreign_keys moved outside transaction for correct behavior
- **base64 Engine API**: Pinned to >=0.21 for stable Engine API

### Changed

- **Homebrew/Scoop Priority**: Installation docs now prioritize package managers over curl
- **Dependency Updates**: Upgraded to latest stable versions of all dependencies

---

## [0.1.31] - 2025-12-01

### Added
- **Vim-style Navigation**: Use `h`/`j`/`k`/`l` (or `Alt`+keys) to navigate between panes and select items in the TUI.
- **Manual Refresh**: Press `Ctrl+Shift+R` to trigger a background re-index without restarting the application.
- **Hidden Pane Indicators**: Visual arrows (`◀ +2`, `+3 ▶`) now show when agent panes are scrolled out of view.
- **Autocomplete**: Agent filter (`F3`) now shows a dropdown with matching agent names.
- **Line Number Navigation**: Search results now track exact line numbers, allowing precise jumps when opening in an editor (`F8`).
- **Time Chips**: Filter chips now display human-readable dates (e.g., "Nov 25") instead of raw timestamps.
- **Reset State**: `Ctrl+Shift+Del` now resets the TUI state (clears history, filters, layout preferences) to defaults.

### Fixed
- **Binary Name**: Fixed error messages referencing incorrect binary name (`coding-agent-search` -> `cass`).
- **Unsafe Code**: Removed unsafe `transmute` usage in UI rendering code.
- **Editor Fallback**: Removed fragile snippet parsing for line numbers; now uses robust index data.
- **Status Bar**: Cleaned up status bar layout to prevent text overflow and improve readability.

### Changed
- **Help**: Updated help strip and F1 help overlay with new shortcuts.
