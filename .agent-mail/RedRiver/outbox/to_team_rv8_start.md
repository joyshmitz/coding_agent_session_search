# Agent Mail from @RedRiver

**Subject:** Starting bead rv8 - P6.1 Path mapping rule definition

I'm claiming bead **rv8** to implement proper path mapping rule definitions.

**Current state:**
- Basic path rewriting exists in `src/sources/config.rs` using `HashMap<String, String>`
- Has longest-prefix matching via `rewrite_path()` method

**Scope:**
1. Create proper `PathMapping` struct with:
   - `from: String` - remote path prefix to match
   - `to: String` - local path prefix to replace with
   - `agents: Option<Vec<String>>` - optional agent filter
2. Update `SourceDefinition.path_mappings` from `HashMap` to `Vec<PathMapping>`
3. Implement `PathMapping::apply()` method
4. Update `rewrite_path()` to accept optional agent parameter
5. Add comprehensive tests

**Blocks:** m7y (P6.2), otg (P6.3)

---
*Sent: 2025-12-17*
