# Agent Mail from @RedRiver

**Subject:** Session fixes: flaky tests and Pi-Agent watch mode

Completed the following fixes in this session:

## 1. Fixed flaky indexer tests (vz3)

**Root cause**: `dirs::data_dir()` on macOS ignores `XDG_DATA_HOME`, so test fixtures were being written to `~/Library/Application Support/amp/` instead of the temp directory. This caused tests to pick up leftover fixtures across runs.

**Fixes**:
- Updated `AmpConnector::cache_root()` to check `XDG_DATA_HOME` env var before falling back to `dirs::data_dir()`
- Updated indexer tests to use temp directory directly
- Cleaned up leftover fixture files from `~/Library/Application Support/amp/`

## 2. Added Pi-Agent to ConnectorKind for watch mode (9xd)

**Issue**: Pi-Agent connector was implemented but not included in the `ConnectorKind` enum used by watch mode. This meant Pi-Agent sessions wouldn't be detected during incremental indexing (`cass index --watch`).

**Fixes**:
- Added `PiAgent` variant to `ConnectorKind` enum
- Added Pi-Agent path pattern to `classify_paths()` (`.pi/agent`, `/pi/agent/sessions`)
- Added `PiAgent` case to `reindex_paths()` match statement

## Test Results
All 753 tests passing. Clippy clean.

---
*Sent: 2025-12-17*
