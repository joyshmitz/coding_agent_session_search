# 🔎 coding-agent-search

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-blue.svg)
![Rust](https://img.shields.io/badge/Rust-nightly-orange.svg)
![Status](https://img.shields.io/badge/status-alpha-purple.svg)

Unified TUI to search and browse local coding-agent history (Codex, Claude Code, Gemini CLI, Cline, OpenCode, Amp) with fast indexing, filters, and a colorful terminal experience.

<div align="center">

```bash
# Fast path: checksum-verified install + self-test
curl -fsSL https://raw.githubusercontent.com/Dicklesworthstone/coding_agent_session_search/main/install.sh \
  | bash -s -- --easy-mode --verify
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/Dicklesworthstone/coding_agent_session_search/main/install.ps1 | iex
install.ps1 -EasyMode -Verify
```

</div>

---

## ✨ Highlights
- **Three-pane TUI** (ratatui): live search, filter pills, rich detail view, open-in-editor, help overlay, light/dark themes.
- **Connectors** for Codex, Claude Code, Gemini CLI, Cline (VS Code), OpenCode, Amp; incremental since_ts ingestion; source paths preserved.
- **Indexing pipeline**: normalized SQLite + Tantivy; FTS5 mirror; append-only updates; watch-mode with mtime routing and watch_state persistence.
- **Search**: multi-field (title/content) with agent/workspace/time filters, pagination, snippets, and Tantivy fallback to SQLite when needed.
- **Logging & tracing**: spans for connectors/indexer/search to aid debugging and tests.
- **Installer**: curl|bash or pwsh with checksum enforcement, optional artifact override, easy/normal modes, rustup nightly bootstrap, PATH hints, self-test and quickstart hooks.
- **Tests & CI**: unit, connector fixtures, storage/indexer/search/TUI snapshots, installer e2e (file:// artifacts), headless TUI smoke; CI runs fmt/clippy/check/test + e2e.

## 🚀 Quickstart
1) **Install** (easy-mode shown):
   ```bash
   curl -fsSL https://raw.githubusercontent.com/Dicklesworthstone/coding_agent_session_search/main/install.sh \
     | bash -s -- --easy-mode --verify
   ```
   - Flags: `--dest DIR`, `--system`, `--artifact-url`, `--checksum`, `--checksum-url`, `--quickstart`, `--quiet`.
   - Skipping rustup: set `RUSTUP_INIT_SKIP=1` (for environments with nightly already installed).
2) **Index** your logs:
   ```bash
   coding-agent-search index --full
   # optional: --data-dir /path/to/state
   ```
3) **Launch TUI**:
   ```bash
   coding-agent-search tui
   # headless smoke: TUI_HEADLESS=1 coding-agent-search tui --once --data-dir <dir>
   ```

### TUI keymap (current)
- Search: type; `/` focuses query. Ctrl-R cycles query history.
- Filters: `F3` agent, `F4` workspace, `F5/F6` time range; `Shift+F3` scope to active agent; `Shift+F4` clear agent scope; `Shift+F5` cycle time presets (24h/7d/30d/all); `F11` clear all filters; backspace on empty query peels last filter.
- Modes: `F9` toggles match mode (prefix ↔ standard). `F12` cycles ranking (recent-heavy → balanced → relevance-heavy). `F2` toggles theme.
- Context: `F7` cycles context window (S/M/L/XL); `Space` temporarily peeks XL then returns.
- Density: `Shift+=/+` increase per-pane items, `-` decrease (min 4, max 50).
- Navigation: arrows move within pane; Left/Right switch panes; `PgUp/PgDn` paginate; `Alt+NumPad 1-9` jump to pane; `g/G` jump to first/last item in pane.
- Detail tabs: `Tab`/`BackTab` cycle Messages/Snippets/Raw.
- Actions: `Enter`/`F8` open hit in `$EDITOR`; `Esc`/`F10` quit; `F1` toggles help modal.
- Empty state: shows recent per-agent conversations before typing; recent queries list when query is empty.

State: match mode and context window persist in `tui_state.json` under the data dir; delete that file to reset.

## 🛠️ CLI reference
```bash
coding-agent-search index [--full] [--watch] [--data-dir DIR] [--db PATH]
coding-agent-search tui [--data-dir DIR] [--once]
coding-agent-search completions <shell>
coding-agent-search man
```
- **index --full** truncates DB/index (non-destructive to source logs) then re-ingests.
- **index --watch** debounced file watcher; routes changes to matching connector; maintains `watch_state.json`.
- **Data locations**: defaults to platform data dir (`directories`); override with `--data-dir`.

## 🧠 Architecture
- **Model layer**: normalized agents/workspaces/conversations/messages/snippets (`src/model`).
- **Storage**: rusqlite with WAL pragmas, migrations, schema_version, FTS5 mirror; append-only insert/update; rebuild_fts helper.
- **Search**: Tantivy schema (agent, workspace, source_path, msg_idx, created_at, title, content); SQLite FTS fallback.
- **Connectors**: detection + scan with since_ts filtering, external_id dedupe, idx resequencing, workspace/source path propagation.
- **UI**: ratatui layout with filter pills, badges, themed detail pane, status/footer; headless once-mode for CI.

```mermaid
flowchart LR
    classDef pastel fill:#f4f2ff,stroke:#c2b5ff,color:#2e2963;
    classDef pastel2 fill:#e6f7ff,stroke:#9bd5f5,color:#0f3a4d;
    classDef pastel3 fill:#e8fff3,stroke:#9fe3c5,color:#0f3d28;
    classDef pastel4 fill:#fff7e6,stroke:#f2c27f,color:#4d350f;
    classDef pastel5 fill:#ffeef2,stroke:#f5b0c2,color:#4d1f2c;

    subgraph Sources
      A[Codex
      Cline
      Gemini
      Claude
      OpenCode
      Amp]:::pastel
    end

    subgraph Connectors
      C1[Detect & scan<br/>since_ts filtering<br/>external_id dedupe]:::pastel2
    end

    subgraph Storage
      S1[SQLite (WAL)
      schema_version
      migrations]:::pastel3
      S2[FTS5 mirror]:::pastel3
    end

    subgraph Search
      T1[Tantivy index<br/>agent/workspace/content]:::pastel4
      F1[SQLite FTS fallback]:::pastel4
    end

    subgraph UI
      U1[TUI (ratatui)
      filters + detail + help]:::pastel5
      U2[Headless once-mode]:::pastel5
    end

    A --> C1 --> S1
    C1 --> S2
    S1 --> T1
    S2 --> F1
    T1 --> U1
    F1 --> U1
    T1 --> U2
    F1 --> U2
```

## 🔒 Integrity & safety
- Installer requires sha256 verification (auto-fetches `<artifact>.sha256` or uses provided CHECKSUM).
- Temporary workdir + lock per run; no destructive file ops; installs to user-local by default.
- rustup nightly bootstrap only when cargo/nightly missing (skippable via env).

## ⚙️ Environment
- Loads `.env` via `dotenvy::dotenv().ok()`; configure API/base paths there (see pattern in code). Do not overwrite `.env`.
- Default data dir: `directories::ProjectDirs` for `coding-agent-search`; override via flags.

## 🧪 Developer workflow
- `cargo fmt --check`
- `cargo check --all-targets`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `cargo test --test install_scripts -- --test-threads=1`
- `cargo test --test e2e_index_tui -- --test-threads=1`
- `cargo test --test e2e_install_easy -- --test-threads=1`

## 🔍 Connectors coverage
- **Codex**: `~/.codex/sessions/**/rollout-*.jsonl`
- **Cline**: VS Code globalStorage `saoudrizwan.claude-dev` task dirs
- **Gemini CLI**: `~/.gemini/tmp/**`
- **Claude Code**: `~/.claude/projects/**` + `.claude/.claude.json`
- **OpenCode**: `.opencode` SQLite DBs (project/global)
- **Amp**: VS Code globalStorage + `~/.local/share/amp` caches

## 🩺 Troubleshooting
- **Checksum mismatch**: ensure `.sha256` reachable or pass `--checksum` explicitly; check proxies/firewalls.
- **Binary not on PATH**: append `~/.local/bin` (or your `--dest`) to PATH; re-open shell.
- **Nightly missing in CI**: set `RUSTUP_INIT_SKIP=1` if toolchain is preinstalled; otherwise allow installer to run rustup.
- **Watch mode not triggering**: confirm watch_state.json updates and that connector roots are accessible; mtime-based routing expects real file touch.

## 📜 License
Project license is recorded in the repository (see LICENSE file).

## 🤝 Contributing
- Follow nightly toolchain policy and run fmt/clippy/test before sending changes.
- Keep console output colorful and informative; avoid destructive commands; do not use regex-based mass scripts in this repo.
