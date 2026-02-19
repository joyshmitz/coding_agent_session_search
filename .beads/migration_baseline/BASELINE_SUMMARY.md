# Pre-Migration Baseline Metrics
**Date:** 2026-02-19
**Rust:** 1.93.1 (01f6ddf75 2026-02-11)
**Agent:** DustyCarp (claude-opus-4-6)
**Bead:** coding_agent_session_search-3r4jg

## 1. Binary Size
- **Release binary:** 35,584,616 bytes (33.94 MB)
- Profile: `opt-level = "z"`, LTO, strip, single codegen unit

## 2. Test Suite
- **Unit tests:** 3,473 passed, 0 failed, 2 ignored (43.70s)
- **Connector tests:** 8 passed (0.00s)
- **Search tests:** 86 passed (4.85s)
- **E2E tests:** 60 passed (0.65s)
- **CLI index tests:** 8 passed, 1 failed (known flaky: `incremental_index_only_processes_new_sessions`)
- **Total:** 3,635 passed, 1 known-flaky failure, 2 ignored

## 3. Clippy & Formatting
- **Clippy:** CLEAN (0 warnings, 0 errors)
- **rustfmt:** CLEAN (no formatting issues)

## 4. Dependencies
- **Direct dependencies:** 100 (depth-1 cargo tree lines)
- See `baseline_deps.txt` for full list

## 5. Line Counts

### Search module (18,965 lines total)
| File | Lines |
|------|-------|
| query.rs | 10,118 |
| model_download.rs | 1,316 |
| two_tier_search.rs | 1,238 |
| daemon_client.rs | 1,183 |
| canonicalize.rs | 1,039 |
| embedder_registry.rs | 681 |
| model_manager.rs | 653 |
| reranker_registry.rs | 567 |
| vector_index.rs | 455 |
| hash_embedder.rs | 380 |
| fastembed_embedder.rs | 329 |
| embedder.rs | 272 |
| tantivy.rs | 218 |
| reranker.rs | 216 |
| fastembed_reranker.rs | 210 |
| ann_index.rs | 54 |
| mod.rs | 36 |

### Connectors module (21,421 lines total)
| File | Lines |
|------|-------|
| mod.rs | 2,190 |
| cursor.rs | 2,148 |
| opencode.rs | 2,041 |
| pi_agent.rs | 1,395 (note: counted as codex in table) |
| codex.rs | 1,961 |
| chatgpt.rs | 1,748 |
| gemini.rs | 1,594 |
| claude_code.rs | 1,465 |
| amp.rs | 1,291 |
| cline.rs | 1,108 |
| factory.rs | 1,080 |
| openclaw.rs | 935 |
| copilot.rs | 934 |
| aider.rs | 826 |
| vibe.rs | 378 |
| clawdbot.rs | 327 |

## 6. Benchmark Latencies

### Cache (benches/cache_micro.rs)
| Benchmark | Time (median) |
|-----------|---------------|
| cache_prefix_hit | 30.05 µs |
| typing_forward_5char | 221.57 µs |
| typing_backspace_5char | 208.03 µs |
| rapid_keystroke_mixed_7 | 267.21 µs |
| cache_cold_query | 77.62 µs |

### Crypto (benches/crypto_perf.rs)
| Benchmark | Time (median) |
|-----------|---------------|
| argon2id_minimal | 13.69 ms |
| aes_gcm_encrypt/1KB | 5.70 µs |
| aes_gcm_encrypt/64KB | 320.09 µs |
| aes_gcm_encrypt/1MB | 5.22 ms |
| aes_gcm_encrypt/10MB | 52.52 ms |
| aes_gcm_decrypt/1KB | 8.79 µs |
| aes_gcm_decrypt/64KB | 443.24 µs |
| aes_gcm_decrypt/1MB | 6.88 ms |
| aes_gcm_decrypt/10MB | 56.89 ms |
| aes_gcm_roundtrip/1KB | 11.44 µs |
| aes_gcm_roundtrip/64KB | 684.56 µs |
| aes_gcm_roundtrip/1MB | 11.40 ms |
| hkdf_extract | 1.47 µs |

### Database (benches/db_perf.rs)
| Benchmark | Time (median) |
|-----------|---------------|
| db_open | 708.79 µs |
| db_open_with_1k_convs | 418.04 µs |
| db_open_readonly | 350.21 µs |
| insert_batch/10_convs | 109.93 ms |
| insert_batch/50_convs | 112.63 ms |
| insert_batch/100_convs | 175.14 ms |
| fetch_messages/10_msgs | 19.99 µs |
| fetch_messages/50_msgs | 59.95 µs |
| fetch_messages/100_msgs | 111.57 µs |
| fetch_messages/500_msgs | 499.76 µs |
| list_agents | 12.70 µs |
| list_workspaces | 11.75 µs |
| fts_rebuild/100_convs | 18.91 ms |
| fts_rebuild/500_convs | 95.31 ms |
| fts_rebuild/1000_convs | 192.72 ms |
| daily_histogram_30_days | 19.06 µs |
| session_count_range | 21.63 µs |
| db_scaling/100_convs | 180.92 µs |
| db_scaling/500_convs | 310.70 µs |
| db_scaling/1000_convs | 457.57 µs |
| db_scaling/2500_convs | 945.78 µs |

### Export (benches/export_perf.rs)
| Benchmark | Time (median) |
|-----------|---------------|
| compress_levels/level_1 | 1.28 ms |
| compress_levels/level_6 | 3.45 ms |
| compress_levels/level_9 | 3.50 ms |
| compress_scaling/64KB | 233.54 µs |
| compress_scaling/256KB | 896.16 µs |
| compress_scaling/1MB | 3.62 ms |
| compress_scaling/4MB | 19.95 ms |
| decompress/64KB | 19.48 µs |
| decompress/256KB | 33.73 µs |
| decompress/1MB | 132.04 µs |
| decompress/4MB | 460.37 µs |
| compress_roundtrip/1MB | 3.61 ms |
| compress_roundtrip/4MB | 14.47 ms |
| json_serialize/10_msgs | 5.18 µs |
| json_serialize/50_msgs | 24.34 µs |
| json_serialize/200_msgs | 96.54 µs |
| msgpack_serialize | 518.35 ns |
| msgpack_deserialize | 913.95 ns |

### Index (benches/index_perf.rs) - SKIPPED
- `index_full_empty` estimated >30 hours; skipped as impractical for baseline

## 7. Search Quality (10 representative queries, limit=10)

| # | Query | Hits | Top-3 Scores |
|---|-------|------|--------------|
| 1 | "authentication error" | 10 | 39.34, 36.24, 34.73 |
| 2 | "database migration" | 10 | 56.79, 53.09, 45.23 |
| 3 | "TODO fix" | 10 | 44.94, 38.68, 37.22 |
| 4 | "how to handle rate limiting in async rust" | 10 | 70.39, 58.43, 50.74 |
| 5 | "unwrap panic" | 10 | 61.47, 60.86, 49.76 |
| 6 | "error handling middleware" | 10 | 64.18, 62.61, 61.19 |
| 7 | "tantivy index" | 10 | 46.91, 46.91, 45.53 |
| 8 | "connector parsing JSONL" | 10 | 40.98, 39.83, 35.06 |
| 9 | "encryption AES key derivation" | 10 | 65.28, 63.21, 60.78 |
| 10 | "git commit hook" | 10 | 42.34, 42.22, 41.31 |

## Files in this directory
- `baseline_binary_size.txt` - Binary size with build metadata
- `baseline_tests.log` - Full test suite output
- `baseline_test_summary.txt` - Summarized test results
- `baseline_clippy.log` - Clippy output
- `baseline_fmt.log` - rustfmt output
- `baseline_bench.log` - Full benchmark output (cache, crypto, db, export)
- `baseline_deps.txt` - cargo tree depth-1 output
- `baseline_search_lines.txt` - Search module line counts
- `baseline_connector_lines.txt` - Connector module line counts
- `baseline_search_quality.json` - Machine-readable search quality data
- `search_queries/q01-q10_*.json` - Raw search query results
