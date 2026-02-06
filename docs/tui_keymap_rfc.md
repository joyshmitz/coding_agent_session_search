# TUI Interaction Model & Keymap RFC (cass)

Status: draft  
Owner: RedHill  
Related issues: coding_agent_session_search-2noh9.1.4, coding_agent_session_search-2noh9.1.5, coding_agent_session_search-2noh9.2.2

## Principles
- Keyboard-first; mouse optional, never required.
- Consistency: same chord does the same thing across panes; avoid mode confusion.
- Discoverability: contextual help strip + command palette make actions findable.
- Safety: no destructive actions without confirmation; ESC always exits modals/search.
- Terminal resilience: graceful degradation on limited key support (esp. Ctrl+arrows, function keys).

## Global Keymap
- `Ctrl+P` — Command palette (fuzzy actions)
- `?` — Quick tour / help overlay
- `F1` — Toggle help strip pin/unpin (if available)
- `Esc` — Close modal/overlay; clear inline searches; cancel prompts
- `Tab` / `Shift+Tab` — Cycle focus panes (search → results → detail → footer/help)
- `Ctrl+S` — Save view (see Saved Views)
- `Ctrl+R` — Reload index/view state (non-destructive)
- `F12` — Cycle ranking mode (recent / balanced / relevance / quality)
- `Ctrl+B` — Toggle rounded/plain borders
- `D` — Cycle density (Compact/Cozy/Spacious)
- `g` / `G` — Jump to top/bottom of list
- `PageUp/PageDown` or `Ctrl+U/Ctrl+D` — Scroll results pane by page

## Search Bar
- Direct typing — live query
- `Up/Down` — Navigate query history
- `Ctrl+L` — Clear search query
- `Ctrl+W` — Delete last token
- `Enter` — Run search immediately (forces query even during debounce)
- `Ctrl+F` — Toggle wildcard fallback indicator (UI only; does not change query)

## Filters & Pills
- `F3` — Agent filter picker
- `F4` — Workspace filter picker
- `F5` — Time filter cycle (today/week/30d/all)
- `F6` — Custom date range prompt
- `F10` — Ranking mode cycle (alias of F12 if function keys limited)
- In pills: `Enter` to edit, `Backspace` to remove, `Left/Right` to move between pills
- Mouse: click pill to edit/remove

## Results Pane
- `Up/Down` — Move selection
- `Enter` — Open drill-in modal (thread view)
- `m` — Toggle multi-select
- `Space` — Peek XL context (tap again to restore)
- `A` — Bulk actions menu (open all, copy paths, export JSON, tag)
- `/` — In-pane quick filter; `Esc` clears
- `y` — Copy current snippet to clipboard
- `o` — Open source file in $EDITOR
- `v` — View raw source (non-interactive)
- `r` — Refresh results (re-run query)

## Detail Pane (Drill-In Modal)
- `Left/Right` — Switch tabs (Messages / Snippets / Raw)
- `c` — Copy path
- `y` — Copy selected snippet
- `o` — Open in $EDITOR
- `f` — Toggle wrap in detail view
- `Esc` — Close modal, return focus to results

## Saved Views
- `Ctrl+1..9` — Save current filters/ranking to slot
- `Shift+1..9` — Recall slot
- Toast confirms save/load; errors surface in footer

## Density & Theme
- `D` — Cycle density presets
- `Ctrl+T` — Toggle theme (dark/light)
- `F2` — Theme toggle (legacy alias)

## Update Assistant
- When banner shown: `U` upgrade, `s` skip this version, `d` view notes, `Esc` dismiss

## Minimal / Robot Mode Behavior
- When `TUI_HEADLESS=1` or stdout non-TTY: disable command palette, animation, icons; only allow `search`/`stats`/`view` via CLI.
- Shortcut hints hidden; actions reachable via flags.

## Fallbacks for Limited Key Support
- If function keys unavailable: map F3/F4/F5/F6 to `Ctrl+3/4/5/6`.
- If Ctrl+P conflicts: palette alias `Alt+P`.
- If clipboard unsupported: `y` writes to temporary file path displayed in footer.

## Mouse (optional)
- Click focus between panes.
- Scroll wheel scrolls results/detail.
- Click pill to edit/remove; click breadcrumb to change scope.
- Drag not required anywhere.

## Safety / Destructive Actions
- Bulk operations that open files only; no delete actions exist.
- Any future destructive command must confirm via y/N prompt; default No.

## Acceptance (coding_agent_session_search-002)
- Keymap is conflict-free, discoverable (help strip + palette), and defined for degraded terminals.
- ESC always backs out safely; no orphaned modal states.
- Saved view, density, and theme toggles have keybindings and documented fallbacks.
- Update assistant keys defined.

## FrankenTUI Architecture Mapping (coding_agent_session_search-2noh9.1.4)

### Core Mapping Table

| Concern | Current cass (ratatui/crossterm) | Target cass (ftui) | Implementation Notes |
|---|---|---|---|
| Terminal lifecycle | Manual raw mode + alt-screen setup/teardown in `src/ui/tui.rs` | `ftui_core::terminal_session::TerminalSession` + `SessionOptions` | Centralize startup/shutdown in one session owner; guarantees cleanup on panic/exit paths. |
| Screen mode | Fullscreen/alt-screen only | `ftui_runtime::ScreenMode::{Fullscreen, Inline { ui_height }}` + `UiAnchor` | Enables inline mode without custom terminal hacks; preserve fullscreen default. |
| Render pipeline | Immediate-mode drawing via ratatui frame | `ftui_render::Frame` -> `BufferDiff` -> `Presenter` | Move to deterministic diff-based rendering and reduce terminal write churn. |
| Event model | `crossterm::event::Event` handled directly in imperative loop | `ftui_core::event::Event` consumed by `Program` update loop | Keep one conversion boundary; all feature logic receives normalized ftui events. |
| Runtime orchestration | Hand-rolled poll loop + ad hoc async channels | `ftui_runtime::{Program, Model, Cmd, Subscription}` | Make side effects explicit (`Cmd`) and composable; simplify cancellation/debounce behavior. |
| Layout system | Ratatui `Layout` split + hardcoded constraints | `ftui_layout::{Flex, Grid, Constraint, LayoutSizeHint}` | Replace percentage-only splits with intrinsic/responsive layout and explicit breakpoints. |
| Widget primitives | Ratatui widgets + custom drawing functions | `ftui_widgets::{Widget, StatefulWidget}` + targeted built-ins | Prefer built-ins to reduce cass-specific UI code volume and increase testability. |
| Command palette | Custom palette state/render code | `ftui_widgets::command_palette` | Keep keybinding contract, swap internals to standard widget. |
| Help system | Custom help strip/overlay renderer | `ftui_widgets::{help, help_registry, hint_ranker}` | Preserve discoverability while making context ranking first-class. |
| Results virtualization | Manual list rendering + paging state | `ftui_widgets::VirtualizedList` | Scale to large result sets with bounded render cost. |
| Modal/toast stack | Cass-owned overlay/toast manager | `ftui_widgets::{modal, toast, notification_queue}` | Keep ESC/back semantics while standardizing stack behavior. |
| Focus traversal | Manual focus enum + ad hoc transitions | `ftui_widgets::focus::{FocusGraph, FocusManager}` | Define one focus graph for panes, modals, and command palette. |
| Testing harness | Ratatui backend tests + smoke e2e | `ftui-harness` snapshots + `ProgramSimulator` + render traces | Deterministic snapshot and state-transition testing becomes primary path. |
| Debug traceability | Mixed logs + hand-debugging | `ftui_runtime::{render_trace, input_macro, AsciicastRecorder}` | Capture replayable traces for flaky keyflow and rendering bugs. |

### Widget Adoption Plan (Use Built-ins First)

| Existing cass surface | Preferred ftui widget/module | Fallback if gap remains |
|---|---|---|
| Query/filter top bar | `input`, `group`, `status_line`, `help_registry` | Local wrapper in `src/ui/components` |
| Results pane | `VirtualizedList`, `scrollbar`, `list` | Keep temporary local renderer until feature parity |
| Detail pane | `paragraph`, `tree`, `json_view`, `log_viewer` | Local detail renderer while migrating tabs incrementally |
| Bulk actions / menus | `command_palette`, `modal` | Local modal wrapper with same action contract |
| Notifications | `toast` + `notification_queue` | Existing toast manager (temporary) |

### Design Decisions (Locked for Migration)

1. Use a single top-level `Model` with explicit sub-state enums rather than multiple independent runtime programs.
2. Use `FocusGraph` as the authoritative focus-routing mechanism for `Tab`/`Shift+Tab` and modal focus restore.
3. Keep current keybinding contract stable during migration; behavior changes must be explicit RFC updates.
4. Use built-in ftui widgets for palette/help/modal/virtualized list whenever available; do not re-implement equivalents.
5. Start with explicit state updates in `update()` and only adopt ftui reactive bindings in targeted areas where it reduces complexity.

### Gap-Handling Policy

- If a missing capability is generally reusable, upstream to FrankenTUI and consume via pinned git revision.
- If the behavior is cass-specific, implement a thin local adapter/wrapper in cass.
- Never block migration on "perfect widget parity" if a temporary local wrapper can preserve user behavior.
- Record each gap as a bead linked to the owning migration epic, with explicit exit criteria.

### Migration Execution Order

1. Foundation (`2noh9.2.x`): dependency + runtime skeleton + terminal/session wiring.
2. Parity (`2noh9.3.x`): search/filter/results/detail/modals reimplemented on ftui.
3. Enhancements (`2noh9.4.x`): inline mode, traces, advanced UX, dashboards.
4. QA and removal (`2noh9.5.x`, `2noh9.6.x`): test hardening, then remove ratatui.

### Acceptance for This Mapping Bead

- Mapping covers terminal lifecycle, screen modes, render pipeline, event model, layout, widgets, runtime, and testing hooks.
- Includes explicit decisions for model structure, focus strategy, reactive usage, and built-in widget adoption.
- Includes a concrete gap policy so implementation beads can proceed without reopening architecture debates.

## Finalized Interaction Contract (coding_agent_session_search-2noh9.1.5)

### Context Keymap Matrix (Conflict-Free by Scope)

| Context | Key | Action |
|---|---|---|
| Global | `F1` / `?` | Toggle help |
| Global | `F2` | Toggle theme |
| Global | `F3` / `F4` | Agent / workspace picker |
| Global | `F5` / `F6` | Date-from / date-to filter prompts |
| Global | `F7` | Context window size cycle |
| Global | `F8` | Open selected item in editor |
| Global | `F9` | Match mode cycle |
| Global | `F10` | Quit alias |
| Global | `F12` | Ranking mode cycle |
| Global | `Alt+S` | Search mode cycle (lexical/semantic/hybrid) |
| Global | `Ctrl+Shift+R` | Refresh/re-index action |
| Global | `Ctrl+Del` | Clear all filters |
| Global | `Ctrl+Shift+Del` | Reset persisted TUI state |
| Global | `Tab` / `Shift+Tab` | Focus traversal |
| Global | `Alt+h/j/k/l` | Pane navigation |
| Results list | `Up/Down` | Move selection |
| Results list | `Enter` | Open detail for selected item |
| Results list | `Ctrl+M` | Toggle selection |
| Results list | `A` | Bulk actions |
| Results list | `y` | Copy current item |
| Results list | `Home` / `End` | Jump first / last result |
| Search input | `Ctrl+n` / `Ctrl+p` | History next / previous |
| Search input | `Ctrl+R` | History cycle |
| Search input | `Enter` | Force search now |
| Filter scope | `Shift+F3` / `Shift+F4` | Scope/clear agent/workspace |
| Filter scope | `Shift+F5` | Cycle time presets |
| In-pane find | `/` | Enter local find/filter mode in focused pane |
| Detail view | `[` / `]` or `Left/Right` | Tab switch (Messages/Snippets/Raw) |
| Detail view | `Esc` | Close detail and restore previous focus |

### Reconciliation with `src/ui/shortcuts.rs`

| Constant | Current Value | Final Contract | Status |
|---|---|---|---|
| `HELP`, `THEME`, `FILTER_*`, `CONTEXT_WINDOW`, `EDITOR`, `MATCH_MODE`, `SEARCH_MODE`, `QUIT`, `CLEAR_FILTERS`, `RESET_STATE`, `RANKING`, `REFRESH` | existing | unchanged | keep |
| `DETAIL_OPEN`, `DETAIL_CLOSE` | `Enter`, `Esc` | unchanged | keep |
| `HISTORY_NEXT`, `HISTORY_PREV`, `HISTORY_CYCLE` | `Ctrl+n`, `Ctrl+p`, `Ctrl+R` | unchanged | keep |
| `SCOPE_AGENT`, `SCOPE_WORKSPACE`, `CYCLE_TIME_PRESETS` | `Shift+F3/F4/F5` | unchanged | keep |
| `COPY`, `BULK_MENU`, `PANE_FILTER`, `TAB_FOCUS`, `VIM_NAV`, `JUMP_TOP`, `JUMP_BOTTOM` | existing | unchanged | keep |
| `TOGGLE_SELECT` | `Ctrl+X` | `Ctrl+M` primary, `Ctrl+X` optional legacy alias | intentional change |
| `FOCUS_QUERY` and `PANE_FILTER` | both `/` | `/` stays context-sensitive (search vs pane-local find) | intentional clarification |

### Focus Model

1. Base traversal order: `SearchInput -> ResultsList -> DetailPane -> FooterHelp`.
2. Opening a modal/palette pushes current focus onto a focus stack.
3. Closing modal/palette pops stack and restores exact prior focus node.
4. Detail-local find (`/`) does not change global pane focus; it creates a local sub-focus mode inside `DetailPane`.
5. Results-local filter (`/`) similarly remains scoped to `ResultsList`.

### Modal and Overlay Semantics

1. Modal stack priority: `Prompt > ExportModal > DetailModal > CommandPalette > HelpOverlay`.
2. Only the top overlay receives input.
3. Background panes continue rendering but are not interactive while an overlay is active.
4. Closing the top overlay never mutates lower overlay state.

### ESC Unwind Contract

On `Esc`, unwind in this exact order until one action succeeds:
1. Cancel active text prompt input.
2. Close top-most modal/overlay.
3. Exit pane-local find mode (`ResultsList` or `DetailPane`).
4. Clear transient selection mode if explicitly active.
5. If no pending state remains, quit TUI session.

### Headless / Robot Constraints

When `TUI_HEADLESS=1` (or non-interactive CI path):
- Disable command palette, modal animation, mouse capture, and decorative effects.
- Do not require function keys; all CI flows must be scriptable by CLI flags.
- Keep deterministic render path for snapshots/smoke tests.
- Treat overlay-only actions as no-ops in headless test mode unless explicitly invoked by test harness APIs.

### Degraded Terminal Fallbacks

- If function keys are unavailable, fallback aliases are mandatory (`Ctrl+3/4/5/6` for filter groups, `Alt+P` for palette).
- If clipboard integration is unavailable, copy actions must return an explicit fallback artifact path.
- If terminal width is narrow, preserve keybindings but collapse visual affordances (not behaviors).

### Acceptance for This Keymap Bead

- Every shortcut in `src/ui/shortcuts.rs` is accounted for with keep/change rationale.
- No duplicate meaning exists for a shortcut within the same interaction context.
- Focus traversal, modal stack, and ESC unwind behavior are deterministic and documented.
- Headless and degraded-terminal behaviors are explicit and testable.
