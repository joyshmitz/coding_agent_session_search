# Plan: Compute Comprehensive Analytics Stats (Tokens, Tools, Roles, Time) in CASS

> Prompt that kicked this off:
>
> ```
> vinchinzu (@vin6716): are you capturing any token metrics on any projects? to benchmark your own inteligence per MM token or $
> Jeffrey Emanuel (@doodlestein): I guess cass is grabbing all that stuff. I should build those analytics directly into cass.
> ```

## Executive Summary

Add a **token + usage analytics pipeline** to `cass` that can answer questions like:

- Total tokens per **hour/day/week/month** (historical)
- Breakdowns across **agent types** (codex, claude_code, gemini, cursor, etc.)
- Breakdowns across **projects/workspaces** (and optionally sources/remotes)
- Averages like:
  - tokens per **human message**
  - tokens per **agent response**
  - tokens per **tool call**
  - tokens per **plan**
- Coverage/quality metrics (what % is real API usage vs estimation)

The core idea is:

1. Compute **per-message metrics once** at ingest time (or via backfill).
2. Store those metrics in a **narrow fact table** (no giant content blobs).
3. Maintain **hourly + daily rollup tables** via batched upserts for O(1) / O(#buckets) time-series queries.
4. Prefer **API-provided token usage** when available (e.g., Claude Code usage blocks), otherwise fall back to a deterministic estimate (existing `~chars/4` heuristic) while tracking quality explicitly.

No UI work in this plan; focus is compute + storage.

## 1. Existing Code + Why This Is Straightforward

Key facts from the current architecture:

- Connectors normalize conversations into `NormalizedConversation` / `NormalizedMessage` with:
  - `role` (user/assistant/agent/tool/system/unknown)
  - `content` (flattened text; tool-use blocks are flattened to `[Tool: X]` markers)
  - `extra` (raw per-agent JSON payload, often containing rich metadata)
- Indexer persists into SQLite (`conversations`, `messages`, etc.) and Tantivy.
- There is already a derived aggregation table `daily_stats` used for fast “sessions/messages/chars per day”.
- **Important**: `src/connectors/mod.rs` already contains token extraction utilities:
  - `extract_claude_code_tokens(extra)` parses Claude Code `message.usage`
  - `extract_codex_tokens(extra)` parses Codex `event_msg` `token_count` payload
  - `estimate_tokens_from_content(content, role)` does the deterministic `chars/4` fallback
  - `extract_tokens_for_agent(agent_slug, extra, content, role)` dispatches + preserves model/provider + tool counts

So we do not need to invent extraction; we need to:

- Persist extracted usage in SQLite
- Add time-bucketed rollups for fast analytics queries
- Fix ingestion gaps (Codex token_count events are currently skipped by the Codex connector)

## 2. Definitions (Avoid “One Token Number” Confusion)

### 2.1 Two Kinds of Token Metrics

We should store and expose **two distinct token notions**:

1. **API usage tokens** (cost/compute relevant):
   - Comes from agent logs that include provider usage (e.g., Claude Code `message.usage`)
   - Has components like `input_tokens`, `output_tokens`, and sometimes cache tokens
   - Represents tokens consumed by the provider call, not just the visible message text

2. **Content tokens (estimated)** (message-size / corpus-volume relevant):
   - Deterministic estimate from message content using `chars/4` (already implemented)
   - Applies to every message, across all connectors, uniformly
   - Useful for “tokens per human message” in a consistent way

For “benchmark intelligence per MM token or $”, API usage tokens are what we ultimately want.
For per-message and per-role averages across heterogeneous agents, content-token estimates are often more stable.

### 2.2 Time Buckets

Use integer bucket ids for compactness and index efficiency:

- `hour_id`: hours since 2020-01-01 00:00:00 UTC
- `day_id`: days since 2020-01-01 00:00:00 UTC (already used by `daily_stats`)

Weeks/months can be computed from days quickly, but we can also materialize them later if needed.

### 2.3 Dimensions (What We Want to Slice By)

Minimum viable dimensions:

- `agent_slug` (claude_code, codex, cursor, gemini, aider, etc.)
- `workspace_id` (project path), with a sentinel for unknown
- `source_id` (local vs remote host/source), already present on conversations
- `role` (user/assistant/tool/system/other)

Optional expansion dims (phase 2+):

- `model` (raw model string) and normalized (provider/family/tier)
- `tool_name` (bash/read/etc) for tool-call analytics

## 3. Storage Plan (SQLite)

### 3.1 New Tables

Add narrow analytics tables designed for cheap range scans and aggregation.

#### 3.1.1 `dim_model` (optional but recommended)

Purpose: de-duplicate model strings and enable stable grouping.

Schema sketch:

- `id INTEGER PRIMARY KEY`
- `raw TEXT NOT NULL UNIQUE` (e.g., `claude-sonnet-4-5-20250929`)
- `provider TEXT NOT NULL` (anthropic/openai/google/unknown)
- `family TEXT NOT NULL` (claude/gpt/gemini/unknown)
- `tier TEXT NOT NULL` (sonnet/opus/flash/o3/etc)

Populate via `connectors::normalize_model(raw)`.

If we want to keep v1 simpler, we can store `model_raw` directly in message metrics and add `dim_model` later.

#### 3.1.2 `message_metrics`

One row per message id. This is the *analytics source-of-truth* (analogous to “events”).

Core columns:

- keys / dims:
  - `message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE`
  - `created_at_ms INTEGER NOT NULL` (fallback to conversation started_at if message ts missing)
  - `hour_id INTEGER NOT NULL`
  - `day_id INTEGER NOT NULL`
  - `agent_slug TEXT NOT NULL`
  - `workspace_id INTEGER NOT NULL` (0 = unknown)
  - `source_id TEXT NOT NULL` (e.g., local, work-laptop)
  - `role TEXT NOT NULL` (user/assistant/tool/system/other)
  - `model_id INTEGER` (nullable, if using dim_model)

- content-size metrics:
  - `content_chars INTEGER NOT NULL`
  - `content_tokens_est INTEGER NOT NULL` (chars/4)

- API usage metrics (nullable):
  - `api_input_tokens INTEGER`
  - `api_output_tokens INTEGER`
  - `api_cache_read_tokens INTEGER`
  - `api_cache_creation_tokens INTEGER`
  - `api_thinking_tokens INTEGER`
  - `api_service_tier TEXT`
  - `api_data_source TEXT NOT NULL` (`api` or `estimated`)
    - Note: `api_data_source=estimated` means “no API usage found”, but we still have content estimate.

- tool / plan flags (phase 1 should at least include the counts we already extract):
  - `tool_call_count INTEGER NOT NULL`
  - `has_tool_calls INTEGER NOT NULL` (0/1)
  - `has_plan INTEGER NOT NULL` (0/1) (computed by a cheap heuristic)

Implementation note: keep this table `WITHOUT ROWID` if we make the PK composite; since it’s keyed by `message_id` only, regular table is fine.

#### 3.1.3 `usage_hourly` rollup

Keyed by `(hour_id, agent_slug, workspace_id, source_id)` with sums + counts.

Suggested columns (start minimal, add more as needed):

- keys:
  - `hour_id INTEGER NOT NULL`
  - `agent_slug TEXT NOT NULL`
  - `workspace_id INTEGER NOT NULL`
  - `source_id TEXT NOT NULL`

- counts:
  - `message_count INTEGER NOT NULL`
  - `user_message_count INTEGER NOT NULL`
  - `assistant_message_count INTEGER NOT NULL`
  - `tool_call_count INTEGER NOT NULL`
  - `api_coverage_message_count INTEGER NOT NULL` (messages with api_* present)

- content-estimated tokens:
  - `content_tokens_est_total INTEGER NOT NULL`
  - `content_tokens_est_user INTEGER NOT NULL`
  - `content_tokens_est_assistant INTEGER NOT NULL`

- API tokens:
  - `api_tokens_total INTEGER NOT NULL`
  - `api_input_tokens_total INTEGER NOT NULL`
  - `api_output_tokens_total INTEGER NOT NULL`
  - `api_cache_read_tokens_total INTEGER NOT NULL`
  - `api_cache_creation_tokens_total INTEGER NOT NULL`

Primary key:

- `PRIMARY KEY (hour_id, agent_slug, workspace_id, source_id)`

Indexes:

- `(agent_slug, hour_id)`
- `(workspace_id, hour_id)`
- `(source_id, hour_id)`

#### 3.1.4 `usage_daily` rollup

Same as hourly but keyed by `day_id`. This makes week/month trivial and avoids summing 24x rows for daily.

Primary key:

- `PRIMARY KEY (day_id, agent_slug, workspace_id, source_id)`

### 3.2 Why Both Fact Table + Rollups?

- `message_metrics` allows:
  - rebuilding rollups cheaply without touching giant message content
  - debugging correctness (sample a day/hour and verify rollup equals sum of events)
  - adding new rollups/metrics later without re-parsing source agent logs
- `usage_hourly` / `usage_daily` gives:
  - time-series queries in O(#buckets) with tiny rows
  - fast top-N breakdowns without scanning millions of messages

### 3.3 Storage Efficiency Notes

- Keep analytics tables narrow: avoid JSON blobs.
- Prefer ints, small text dims, and surrogate ids for models if needed.
- Consider `STRICT` tables if SQLite version supports it (optional).

## 4. Ingestion Plan (Live, Incremental, Ultra Efficient)

### 4.1 Where To Hook In

All messages flow through SQLite insert points:

- `SqliteStorage::insert_conversation_tree` (new conversation)
- `SqliteStorage::append_messages` (existing conversation, new messages appended)
- `SqliteStorage::insert_conversations_batched` / `insert_conversation_in_tx_batched` (fast path used by indexer)

We should update the batched path first, since that’s the primary ingestion path.

### 4.2 Metric Extraction Per Inserted Message

When a message is inserted (we have `conv.agent_slug`, `conv.source_id`, `workspace_id`, `msg.role`, `msg.content`, `msg.extra_json`):

1. Determine `created_at_ms`:
   - `msg.created_at` if present
   - else fallback to `conv.started_at`
   - else fallback to `SqliteStorage::now_millis()` (last resort; mark as low quality if desired)

2. Compute bucket ids:
   - `day_id = SqliteStorage::day_id_from_millis(created_at_ms)`
   - `hour_id = (created_at_ms/1000 - EPOCH_2020_SECS) / 3600` (add helper `hour_id_from_millis`)

3. Compute content metrics:
   - `content_chars = msg.content.len()`
   - `content_tokens_est = content_chars / 4`

4. Extract API token usage (or fallback) using existing code:
   - `usage = connectors::extract_tokens_for_agent(&conv.agent_slug, &msg.extra_json, &msg.content, role_str)`
   - Persist the fields and also persist `usage.data_source` so we can compute coverage.

5. Heuristic flags:
   - `has_plan`: cheap heuristic (phase 1):
     - true if content contains a "Plan:" header, "## Plan", or starts with "Plan" and has numbered steps
     - intentionally simple; refine later

### 4.3 Batched Rollup Updates (Critical For Speed)

Do NOT upsert per message. Instead:

- While inserting a batch of messages, accumulate deltas in a `HashMap<(bucket_id, agent_slug, workspace_id, source_id), DeltaStruct>`
- At the end of the transaction, flush to `usage_hourly` and `usage_daily` via **multi-value INSERT with ON CONFLICT DO UPDATE**.

This matches existing performance patterns:

- `batch_insert_fts_messages` already does multi-value insert with fallback.
- `daily_stats` update path already has a batched aggregator (`StatsAggregator`).

We should mirror that design for token usage.

### 4.4 “All” Rows vs Group-By

We have two options:

1. Store only exact dims and do `SUM(...) GROUP BY ...` over rollup table.
2. Store permutation rows like `agent_slug='all'`, `workspace_id=0 (all)`, `source_id='all'` for instant totals.

Recommendation:

- Start with exact dims only.
- If we find a real performance need, add permutation rows later using the same “expand” strategy as `daily_stats`.

Summing thousands of rollup rows is already cheap, and it avoids 4x/8x row count inflation.

## 5. Connector Gaps To Fix (For Real API Tokens + Tool Metrics)

### 5.1 Codex token_count events are currently skipped

`extract_codex_tokens` expects `event_msg.payload.type == token_count`, but `CodexConnector::scan` currently ignores those event types.

Plan:

- Update Codex connector parsing so token_count events are not discarded.
- Best approach: attach token_count data to the nearest preceding assistant response item in `NormalizedMessage.extra` under a `cass` namespace, e.g.:
  - `extra["cass"]["token_usage"]["output_tokens"] = ...`
  - then extend `extract_codex_tokens` to read from that location as well

This avoids polluting the searchable message stream with token-only synthetic messages.

### 5.2 Tool calls and tool results (cross-agent)

We want “tokens per tool call” and “tool call counts by tool name”.

Phase 1:

- Use what we already extract for Claude Code:
  - tool_use blocks counted from `/message/content` where `type == tool_use`
- Store `tool_call_count` and `has_tool_calls` in `message_metrics`
- Roll up tool counts in hourly/daily tables
- Compute derived metrics:
  - `avg_api_tokens_per_tool_call = api_tokens_total / tool_call_count`

Phase 2+:

- Extend extractors for other connectors (Codex tool_call events, Cursor tool calls, etc.)
- Add optional `tool_usage_hourly(tool_name, ...)` table if we want per-tool breakdowns without scanning `message_metrics`.

## 6. Backfill / Rebuild Strategy (Historical)

We need historical tokens across all existing indexed data.

### 6.1 Migration + “cold start”

On DB migrate to new schema version:

- Create new tables (`message_metrics`, rollups, optional dim tables).
- Do NOT automatically rebuild on every startup (could be big).
- Provide explicit commands:
  - `cass analytics rebuild` (or `cass doctor --fix --rebuild-analytics`)
  - `cass analytics status` (shows coverage, last rebuild time, row counts)

### 6.2 Rebuild algorithm

Rebuild from SQLite (not from raw agent files) to make it fast and deterministic:

1. Clear `message_metrics`, `usage_hourly`, `usage_daily` in a transaction.
2. Stream messages joined with dims we need:
   - messages + conversations (source_id) + agents (agent_slug) + workspaces (workspace_id)
3. Process in chunks (e.g., 10k messages per transaction):
   - compute per-message metrics
   - insert into `message_metrics` (batched multi-insert)
   - update rollup aggregators in-memory
   - flush rollup upserts per chunk
4. Record rebuild metadata in `meta`:
   - `analytics_rebuild_completed_at`
   - `analytics_message_metrics_version`
   - coverage stats (optional)

### 6.3 Incremental maintenance (watch mode)

After rebuild, live ingest keeps analytics up-to-date by updating:

- `message_metrics` for new messages
- rollups for new messages only

No rescan required.

## 7. Query Surface (Robot-First)

Even without UI, we should add an API for other agents/tools to consume.

Proposed CLI:

- `cass analytics tokens --group-by hour|day|week|month --since ... --until ...`
- Filters:
  - `--agent <slug>`
  - `--workspace <path>` (resolved to workspace_id)
  - `--source <local|remote|id>`
- Metrics selection:
  - `--metric api_total|api_input|api_output|content_est_total|...`
- Include quality:
  - `% api coverage` per bucket
  - `api_total` vs `content_est_total` side-by-side

Output is JSON in `--robot` mode and should follow existing conventions:

- stdout is data only
- stderr diagnostics
- include optional `_meta` timings / row counts

## 8. “LOTS MORE” Metrics (Designed In, Implement Later)

With `message_metrics` as a fact table, adding new analytics becomes easy:

- Per-model tokens and cost:
  - add `dim_price(model_id, price_in, price_out, effective_from, ...)`
  - compute dollar estimates by joining rollups
- “Conversation structure” metrics:
  - average turns per session, tokens per session, tokens per hour of activity
- Tooling intensity metrics:
  - tool_calls per 1k tokens
  - average tool payload size (estimated)
- Planning metrics:
  - plan frequency and plan token share
- “Noise” metrics:
  - tokens in repeated tool acknowledgments (dedup by content hash)
- Speed metrics (if timestamps available):
  - tokens per minute of wall-clock session time
- Source comparisons:
  - local vs remote machines
  - which machine burns the most tokens

## 9. Testing Strategy

Add tests at three layers:

1. Unit tests (already exist) for extraction:
   - extend `extract_codex_tokens` tests once Codex connector attaches token_count

2. Storage tests:
   - Insert a synthetic conversation with messages containing:
     - user + assistant
     - Claude-style `message.usage`
     - tool_use blocks
   - Assert `message_metrics` rows are created
   - Assert hourly/daily rollups equal summed message_metrics

3. End-to-end (robot) tests:
   - `cass index --full` on fixture logs
   - `cass analytics tokens --group-by day --robot` returns stable output

## 10. Implementation Phases (Concrete)

### Phase 0: Design + Schema

- Add migrations for new tables + meta keys.
- Add helper functions:
  - `hour_id_from_millis`
  - `millis_from_hour_id` (optional)

### Phase 1: Fact Table + Rollups From Ingest

- Implement `message_metrics` inserts in batched ingestion path.
- Implement rollup updates in batched ingestion path.
- Add `cass analytics status` (row counts, coverage).

### Phase 2: Backfill Command

- Add `cass analytics rebuild` (fast rebuild from SQLite).

### Phase 3: Codex token_count wiring

- Modify Codex connector to retain token_count and attach to assistant turns.
- Extend `extract_codex_tokens` accordingly.

### Phase 4: Tool breakdowns + plan heuristics refinement

- Add per-tool rollup (optional) if needed.
- Improve plan detection and add plan token metrics.

## Open Questions

1. Codex `token_count` semantics: output-only, total, or something else? We need to confirm by inspecting real rollout logs.
2. Should we add a real tokenizer (BPE) for content tokens, or keep `chars/4` for now?
3. How aggressively should we denormalize dims into rollups (workspace_id + agent_slug + source_id)? Row count could grow; we should measure on a real corpus.

