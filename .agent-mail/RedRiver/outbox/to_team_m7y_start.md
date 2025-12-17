# Agent Mail from @RedRiver

**Subject:** Starting bead m7y - P6.2 Apply path mappings at ingest time

I'm claiming bead **m7y** to apply workspace path rewriting during indexing.

**Why ingest-time (not display-time):**
If a user filters with `--workspace=/Users/me/projects/myapp`, they should find:
- Local sessions from `/Users/me/projects/myapp`
- Remote sessions with original path `/home/user/projects/myapp`

Ingest-time rewriting ensures filter consistency across sources.

**Scope:**
1. Apply workspace rewrite during normalization in indexer
2. Store original workspace in metadata/column for audit
3. Update Tantivy schema with workspace_original field (STORED)
4. Update SQLite with workspace_original column
5. Update robot output to include both paths
6. Add CLI flag --show-original-paths for display

**Dependencies (all closed):**
- rv8 (P6.1) - PathMapping struct (just completed)
- alb (P3.3) - SearchHit provenance fields
- 1mv (P2.2) - Indexer multi-root orchestration

---
*Sent: 2025-12-17*
