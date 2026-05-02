# Full Rebuild Shard Builder And Merge Fanout Probe

Date: 2026-05-02
Host: `threadripperje`
Baseline commit: `7ff3821e4ceb5ec23073148d068f28e411c6f2b3`
Workload: repair a missing Tantivy lexical index from the 22.4 GB CASS seed DB.

## Workload

Each run used a reflinked copy of:

`/home/ubuntu/cass-post-tokenizer-hotspot-20260502T035907Z/agent_search.db`

Command shape:

```bash
env CASS_RESPONSIVENESS_DISABLE=1 CASS_PREP_PROFILE=1 \
  cass index \
  --watch-once /home/ubuntu/cass-full-rebuild-shard-builders-missing-<label>-20260502T2050Z.jsonl \
  --data-dir /home/ubuntu/cass-full-rebuild-shard-builders-<label>-20260502T2050Z \
  --json --progress-interval-ms 5000
```

The probes additionally set one of:

- `CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS=6`
- `CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS=4`
- `CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS=4`

## Results

| Label | Config | JSON elapsed ms | Wall | Max RSS KB | Conversations | Messages |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `default` | 8 shard builders, 8 merge workers | 44,632 | 0:45.89 | 40,437,724 | 51,214 | 4,711,686 |
| `builders6` | 6 shard builders | 45,432 | 0:46.66 | 39,871,116 | 51,214 | 4,711,686 |
| `builders4` | 4 shard builders | 46,133 | 0:46.90 | 39,313,048 | 51,214 | 4,711,686 |
| `merge4` | 4 merge workers | 45,333 | 0:46.45 | 40,237,364 | 51,214 | 4,711,686 |

## Decision

Rejected. Lower shard-builder fanout saves only about 1.4-2.8% RSS and slows
the rebuild. Lower merge fanout does not materially reduce RSS and also slows
the rebuild. The staged fanout defaults should stay unchanged until a stronger
profile points at a different limit.

This probe is retained because it narrows the next search space: after the
page-prep reduction, staged builder and merge fanout are not the next durable
default-tuning win for this workload.
