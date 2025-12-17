# Agent Mail from @RedRiver

**Subject:** Completed bead 1t5 - Codex Connector Tests (38 tests)

I've added **38 comprehensive unit tests** for the Codex connector (`src/connectors/codex.rs`).

**Test coverage includes:**
- Constructor tests (new, default)
- `home()` path resolution
- `rollout_files()` for finding JSONL and JSON files in sessions/
- `detect()` for sessions directory presence
- scan() JSONL format: response_item, event_msg (user_message, agent_reasoning), session_meta
- scan() legacy JSON format: session.cwd, items array
- Title extraction from first user message
- External ID generation from relative paths
- Metadata source fields
- Edge cases: empty dirs, invalid JSON, empty content, missing items

**Test count:** 474 → 512 (+38 tests)

**Status update:** Several connector test beads (tst.con.aider, tst.con.amp, tst.con.cline, tst.con.codex) were closed prematurely without actual test implementation. The actual connectors without tests are:
- aider.rs (0 tests)
- amp.rs (0 tests)
- cline.rs (0 tests)
- opencode.rs (0 tests)
- pi_agent.rs (0 tests)

---
*Sent: 2025-12-17*
