# Pre-Migration Baseline Metrics

**Captured:** 2026-02-19
**Purpose:** Baseline measurements for FAD migration (bead 1u2f7) and frankensearch migration (bead 2s9fq)

## 1. Binary Size

- **Release binary:** 35,584,616 bytes (34 MB)
- Profile: lto=true, codegen-units=1, strip=true, panic=abort, opt-level=z

## 2. Test Suite

| Category | Passed | Failed | Ignored |
|----------|--------|--------|---------|
| Unit tests (lib) | 3,473 | 0 | 2 |
| Doc tests | 0 | 0 | 0 |
| Bench tests | 0 | 0 | 0 |
| Integration (e2e) | 8 | 0 | 0 |
| Integration (search) | 86 | 0 | 0 |
| Integration (connectors etc) | 60 | 0 | 0 |
| Integration (cli_index) | 8 | 1 (flaky) | 0 |
| **Total** | **3,635** | **1 (known flaky)** | **2** |

Known flaky: `cli_index::incremental_index_only_processes_new_sessions` (pre-existing, not a regression)

## 3. Clippy

- **Status:** CLEAN (exit code 0)
- Flags: `--all-targets -- -D warnings`

## 4. Formatting

- **Status:** CLEAN (exit code 0)
- Command: `cargo fmt --check`

## 5. Dependency Count

- **Direct dependencies:** ~100 entries (depth 1)
- Full tree in `baseline_deps.txt`

## 6. Line Counts

### Search module (src/search/)
| File | Lines |
|------|-------|
| query.rs | 10,118 |
| model_download.rs | 1,316 |
| two_tier_search.rs | 1,238 |
| daemon_client.rs | 1,183 |
| canonicalize.rs | 1,039 |
| model_manager.rs | 653 |
| embedder_registry.rs | 681 |
| vector_index.rs | 455 |
| hash_embedder.rs | 380 |
| fastembed_embedder.rs | 329 |
| embedder.rs | 272 |
| tantivy.rs | 218 |
| reranker.rs | 216 |
| fastembed_reranker.rs | 210 |
| reranker_registry.rs | 567 |
| ann_index.rs | 54 |
| mod.rs | 36 |
| **Total** | **18,965** |

### Connectors module (src/connectors/)
| File | Lines |
|------|-------|
| cursor.rs | 2,148 |
| mod.rs | 2,190 |
| opencode.rs | 2,041 |
| codex.rs | 1,961 |
| chatgpt.rs | 1,748 |
| gemini.rs | 1,594 |
| claude_code.rs | 1,465 |
| pi_agent.rs | 1,395 |
| amp.rs | 1,291 |
| cline.rs | 1,108 |
| factory.rs | 1,080 |
| openclaw.rs | 935 |
| copilot.rs | 934 |
| aider.rs | 826 |
| vibe.rs | 378 |
| clawdbot.rs | 327 |
| **Total** | **21,421** |

## 7. Search Quality (10 representative queries)

| Query | Count | Total | Elapsed(ms) | Top-3 Scores |
|-------|-------|-------|-------------|-------------|
| API endpoint design | 5 | 5 | 27 | [39.387, 38.739, 37.219] |
| TODO fix | 5 | 5 | 25 | [44.937, 38.680, 37.223] |
| authentication error | 5 | 5 | 19 | [39.342, 36.241, 34.732] |
| database migration | 5 | 5 | 22 | [56.786, 53.090, 45.231] |
| docker deployment | 5 | 5 | 16 | [37.174, 21.842, 19.236] |
| git merge conflict | 5 | 5 | 26 | [82.306, 77.096, 76.843] |
| how to handle rate limiting in async rust | 5 | 5 | 69 | [70.393, 58.426, 50.743] |
| memory leak debugging | 5 | 5 | 25 | [34.321, 24.390, 23.939] |
| rust lifetime error | 5 | 5 | 27 | [41.774, 38.212, 36.977] |
| webpack configuration | 5 | 5 | 17 | [37.582, 23.985, 16.963] |

Full JSON outputs in `search_quality/` subdirectory.

## 8. Benchmarks

Benchmarks not run in this baseline capture (require separate `cargo bench` invocation which is time-intensive). Benchmark results from prior runs are available in benches/ directory.

## Files in This Directory

- `binary_size.txt` - Release binary size details
- `baseline_tests.log` - Full test suite output
- `baseline_clippy.txt` - Clippy output
- `baseline_fmt.txt` - Formatting check output
- `baseline_deps.txt` - Dependency tree (depth 1)
- `baseline_search_lines.txt` - Search module line counts
- `baseline_connector_lines.txt` - Connector module line counts
- `search_quality/` - JSON outputs for 10 representative queries
- `SUMMARY.md` - This file
