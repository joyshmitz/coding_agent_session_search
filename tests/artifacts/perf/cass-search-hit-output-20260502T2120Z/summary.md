# Search Full-Output Preview Fast Path

Date: 2026-05-02
Baseline commit: `7ff3821e4ceb5ec23073148d068f28e411c6f2b3`
After binary: current worktree with the untruncated-preview fast path.
Data dir: `/home/ubuntu/cass-full-rebuild-shard-builders-default-20260502T2050Z`

## Baseline Probe

Hit-heavy minimal and summary projections are already fast:

| Workload | Wall | Max RSS KB | Notes |
| --- | ---: | ---: | --- |
| `function --fields minimal --limit 20` | 0:00.02 | 73,444 | 20 hits, 241,394 total matches |
| `function --fields summary --limit 20` | 0:00.02 | 72,380 | 20 hits, 241,394 total matches |

Default full robot output was slow because it requested full content and opened
SQLite to hydrate missing content:

| Workload | Wall | Max RSS KB | Notes |
| --- | ---: | ---: | --- |
| `function --limit 20` before | 0:00.53 | 915,272 | Full output, byte-for-byte baseline file retained |
| `function --limit 20` after | 0:00.52 | 901,860 | Byte-identical output; unchanged because at least one top-20 hit still needs long-content hydration |

## Accepted Workload

For one-hit full-output search, the top hit is a short message whose stored
preview is already exact full content. Pre-patch CASS still opened SQLite; after
the change it does not.

Command shape:

```bash
cass search function --robot \
  --data-dir /home/ubuntu/cass-full-rebuild-shard-builders-default-20260502T2050Z \
  --limit 1 --mode lexical --color=never
```

| Sample | Binary | Wall | Max RSS KB | Output |
| --- | --- | ---: | ---: | --- |
| `function-full-limit1-before` | pre-patch `7ff3821e` | 0:00.55 | 888,540 | 1 hit, 241,394 total matches |
| `function-full-limit1-after-2` | patched | 0:00.05 | 63,308 | byte-identical JSON |

Result: about 11x lower wall time and about 14x lower max RSS for this
short-hit full-output path.

## Change

When Tantivy has no stored full content but its stored preview is non-empty and
not marked truncated, CASS treats that preview as exact full content and skips
SQLite hydration. Long or ambiguous previews still hydrate from SQLite.

The check is conservative: frankensearch appends `…` when it truncates a
preview, so CASS only trusts previews that do not end with that marker. If a real
short message ends with `…`, CASS simply takes the old hydration path.

## Proof

- `function-full-limit1-before.out.json` and
  `function-full-limit1-after-2.out.json` are byte-identical.
- `function-full-1.out.json` and `function-full-after-1.out.json` are
  byte-identical, proving long-hit top-20 behavior stayed unchanged.
- Regression test asserts long content still hydrates past the preview boundary
  and short exact-preview full-content hits keep the SQLite handle closed.

Focused test:

```bash
TMPDIR=/data/tmp env CARGO_TARGET_DIR=/data/tmp/cass-target-shard-builders-20260502T2050Z \
  cargo test --lib tantivy_search_hydrates_long_content_when_content_field_is_not_stored -- --nocapture
```

Required gates:

```bash
TMPDIR=/data/tmp env CARGO_TARGET_DIR=/data/tmp/cass-target-shard-builders-20260502T2050Z cargo check --all-targets
TMPDIR=/data/tmp env CARGO_TARGET_DIR=/data/tmp/cass-target-shard-builders-20260502T2050Z cargo clippy --all-targets -- -D warnings
cargo fmt --check
git diff --check
```
