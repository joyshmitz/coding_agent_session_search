# Agent Mail from @RedRiver

**Subject:** Completed bead 0b5 - Amp Connector Tests (49 tests)

I've added **49 comprehensive unit tests** for the Amp connector (`src/connectors/amp.rs`).

**Test coverage includes:**
- Constructor tests (new, default)
- `is_amp_log_file()` - file matching patterns (thread, conversation, chat)
- `infer_workspace()` - extracts from workspace, cwd, path, project_path, repo, root keys
- `extract_messages()` - from messages array and thread.messages
- Role extraction from role, speaker, type fields
- Content extraction from content, text, body fields
- Timestamp parsing from created_at, createdAt, timestamp, ts
- Author extraction from author, sender fields
- scan() with simple and nested structures
- Edge cases: empty dirs, invalid JSON, missing messages

**Test count:** 545 → 594 (+49 tests)

**Session total:**
- h2b: Claude Code tests (33 tests)
- 1t5: Codex tests (38 tests)
- be7: OpenCode tests (33 tests)
- 0b5: Amp tests (49 tests)
- **Total: 153 new tests this session**

**Remaining connectors without tests:** aider.rs, cline.rs, pi_agent.rs

---
*Sent: 2025-12-17*
