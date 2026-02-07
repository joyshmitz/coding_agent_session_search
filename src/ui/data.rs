use crate::model::types::{Conversation, Message, MessageRole, Workspace};
use crate::storage::sqlite::SqliteStorage;
use crate::ui::components::theme::ThemePalette;
use anyhow::Result;
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// -------------------------------------------------------------------------
// Input Mode
// -------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputMode {
    Query,
    Agent,
    Workspace,
    CreatedFrom,
    CreatedTo,
    PaneFilter,
    /// Inline find within the detail pane (local, non-indexed)
    DetailFind,
}

// -------------------------------------------------------------------------
// Conversation View
// -------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ConversationView {
    pub convo: Conversation,
    pub messages: Vec<Message>,
    pub workspace: Option<Workspace>,
}

// -------------------------------------------------------------------------
// Conversation Cache (P1 Opt 1.3)
// -------------------------------------------------------------------------

/// Cache statistics for monitoring performance.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
}

impl CacheStats {
    /// Get current stats as a tuple: (hits, misses, evictions).
    pub fn get(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        )
    }

    /// Calculate hit rate as a percentage (0.0 - 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Number of cache shards (must be power of 2 for efficient modulo).
const NUM_SHARDS: usize = 16;

/// Default capacity per shard.
const DEFAULT_CAPACITY_PER_SHARD: usize = 256;

/// Sharded LRU cache for ConversationView to reduce lock contention.
///
/// Caching conversation views avoids:
/// - Database queries (conversation + messages)
/// - JSON parsing (metadata_json, extra_json)
///
/// This is particularly beneficial for:
/// - TUI scrolling (repeated access to same results)
/// - Detail view expansion (view -> expand -> view pattern)
pub struct ConversationCache {
    shards: [RwLock<LruCache<u64, Arc<ConversationView>>>; NUM_SHARDS],
    stats: CacheStats,
}

impl ConversationCache {
    /// Create a new cache with the specified capacity per shard.
    pub fn new(capacity_per_shard: usize) -> Self {
        Self {
            shards: std::array::from_fn(|_| {
                RwLock::new(LruCache::new(
                    NonZeroUsize::new(capacity_per_shard).unwrap_or(NonZeroUsize::MIN),
                ))
            }),
            stats: CacheStats::default(),
        }
    }

    /// Hash a source path to a u64 key using fxhash.
    #[inline]
    fn hash_key(source_path: &str) -> u64 {
        fxhash::hash64(source_path)
    }

    /// Get the shard index for a given hash.
    #[inline]
    fn shard_index(hash: u64) -> usize {
        (hash as usize) % NUM_SHARDS
    }

    /// Get a cached conversation view by source path.
    pub fn get(&self, source_path: &str) -> Option<Arc<ConversationView>> {
        let hash = Self::hash_key(source_path);
        let shard_idx = Self::shard_index(hash);
        let mut shard = self.shards[shard_idx].write();

        if let Some(cached) = shard.get(&hash) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(cached))
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a conversation view into the cache.
    pub fn insert(&self, source_path: &str, view: ConversationView) -> Arc<ConversationView> {
        let hash = Self::hash_key(source_path);
        let shard_idx = Self::shard_index(hash);
        let arc = Arc::new(view);

        let mut shard = self.shards[shard_idx].write();
        // Only count eviction if shard is full AND key doesn't already exist
        if shard.len() == shard.cap().get() && !shard.contains(&hash) {
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
        shard.put(hash, Arc::clone(&arc));

        arc
    }

    /// Invalidate a specific cache entry by source path.
    pub fn invalidate(&self, source_path: &str) {
        let hash = Self::hash_key(source_path);
        let shard_idx = Self::shard_index(hash);
        let mut shard = self.shards[shard_idx].write();
        shard.pop(&hash);
    }

    /// Invalidate all cache entries.
    pub fn invalidate_all(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }

    /// Get cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get total number of cached entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Global conversation cache instance.
pub static CONVERSATION_CACHE: Lazy<ConversationCache> = Lazy::new(|| {
    let capacity = dotenvy::var("CASS_CONV_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CAPACITY_PER_SHARD);
    ConversationCache::new(capacity)
});

// -------------------------------------------------------------------------
// Load Conversation (with caching)
// -------------------------------------------------------------------------

/// Load a conversation from the database (bypassing cache).
/// Use `load_conversation` for cached access.
fn load_conversation_uncached(
    storage: &SqliteStorage,
    source_path: &str,
) -> Result<Option<ConversationView>> {
    let mut stmt = storage.raw().prepare(
        "SELECT c.id, a.slug, w.id, w.path, w.display_name, c.external_id, c.title, c.source_path,
                c.started_at, c.ended_at, c.approx_tokens, c.metadata_json, c.source_id, c.origin_host
         FROM conversations c
         JOIN agents a ON c.agent_id = a.id
         LEFT JOIN workspaces w ON c.workspace_id = w.id
         WHERE c.source_path = ?1
         ORDER BY c.started_at DESC LIMIT 1",
    )?;
    let mut rows = stmt.query([source_path])?;
    if let Some(row) = rows.next()? {
        let convo_id: i64 = row.get(0)?;
        let convo = Conversation {
            id: Some(convo_id),
            agent_slug: row.get(1)?,
            workspace: row
                .get::<_, Option<String>>(3)?
                .map(std::path::PathBuf::from),
            external_id: row.get(5)?,
            title: row.get(6)?,
            source_path: std::path::PathBuf::from(row.get::<_, String>(7)?),
            started_at: row.get(8)?,
            ended_at: row.get(9)?,
            approx_tokens: row.get(10)?,
            metadata_json: row
                .get::<_, Option<String>>(11)?
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            messages: Vec::new(),
            source_id: row
                .get::<_, String>(12)
                .unwrap_or_else(|_| "local".to_string()),
            origin_host: row.get(13)?,
        };
        let workspace = row.get::<_, Option<i64>>(2)?.map(|id| Workspace {
            id: Some(id),
            path: convo.workspace.clone().unwrap_or_default(),
            display_name: row.get(4).ok().flatten(),
        });
        let messages = storage.fetch_messages(convo_id)?;
        return Ok(Some(ConversationView {
            convo,
            messages,
            workspace,
        }));
    }
    Ok(None)
}

/// Load a conversation with LRU caching.
///
/// This is the primary function for loading conversations in the TUI.
/// It uses a sharded LRU cache to avoid repeated database queries and
/// JSON parsing for the same conversation.
///
/// Cache behavior:
/// - Hit: Returns cached Arc<ConversationView> (fast path)
/// - Miss: Queries database, parses JSON, caches result
///
/// The cache is keyed by source_path and has a configurable capacity
/// via the CASS_CONV_CACHE_SIZE environment variable (default: 256 per shard,
/// 4096 total entries across 16 shards).
pub fn load_conversation(
    storage: &SqliteStorage,
    source_path: &str,
) -> Result<Option<ConversationView>> {
    // Fast path: check cache first
    if let Some(cached) = CONVERSATION_CACHE.get(source_path) {
        // Clone out of Arc for API compatibility
        return Ok(Some((*cached).clone()));
    }

    // Cache miss: load from database
    let view = load_conversation_uncached(storage, source_path)?;

    // Cache the result if found
    if let Some(v) = view {
        CONVERSATION_CACHE.insert(source_path, v.clone());
        return Ok(Some(v));
    }

    Ok(None)
}

/// Load a conversation with caching, returning Arc for efficiency.
///
/// Use this variant when you need to hold the conversation view for
/// an extended period without cloning.
pub fn load_conversation_arc(
    storage: &SqliteStorage,
    source_path: &str,
) -> Result<Option<Arc<ConversationView>>> {
    // Fast path: check cache first
    if let Some(cached) = CONVERSATION_CACHE.get(source_path) {
        return Ok(Some(cached));
    }

    // Cache miss: load from database
    let view = load_conversation_uncached(storage, source_path)?;

    // Cache and return the Arc
    if let Some(v) = view {
        let arc = CONVERSATION_CACHE.insert(source_path, v);
        return Ok(Some(arc));
    }

    Ok(None)
}

/// Log conversation cache statistics.
///
/// Outputs cache stats at debug level via tracing.
pub fn log_conversation_cache_stats() {
    let (hits, misses, evictions) = CONVERSATION_CACHE.stats().get();
    let hit_rate = CONVERSATION_CACHE.stats().hit_rate();
    let count = CONVERSATION_CACHE.len();

    tracing::debug!(
        target: "cass::perf::conversation_cache",
        hits = hits,
        misses = misses,
        evictions = evictions,
        hit_rate = format!("{:.1}%", hit_rate * 100.0),
        cached_count = count,
        "Conversation cache statistics"
    );
}

pub fn role_style(role: &MessageRole, palette: ThemePalette) -> ftui::Style {
    match role {
        MessageRole::User => ftui::Style::new().fg(palette.user),
        MessageRole::Agent => ftui::Style::new().fg(palette.agent),
        MessageRole::Tool => ftui::Style::new().fg(palette.tool),
        MessageRole::System => ftui::Style::new().fg(palette.system),
        MessageRole::Other(_) => ftui::Style::new().fg(palette.hint),
    }
}

// -------------------------------------------------------------------------
// Shared TUI types (moved from tui.rs to remove ratatui dependency)
// -------------------------------------------------------------------------

/// How search results are ranked and ordered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RankingMode {
    RecentHeavy,
    Balanced,
    RelevanceHeavy,
    MatchQualityHeavy,
    DateNewest,
    DateOldest,
}

/// Format a timestamp as a short human-readable date for filter chips.
/// Shows "Nov 25" for same year, "Nov 25, 2023" for other years.
pub fn format_time_short(ms: i64) -> String {
    use chrono::{DateTime, Datelike, Utc};
    let now = Utc::now();
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| {
            if dt.year() == now.year() {
                dt.format("%b %d").to_string()
            } else {
                dt.format("%b %d, %Y").to_string()
            }
        })
        .unwrap_or_else(|| "?".to_string())
}

// -------------------------------------------------------------------------
// Unit Tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_test_view(id: i64) -> ConversationView {
        ConversationView {
            convo: Conversation {
                id: Some(id),
                agent_slug: "claude".to_string(),
                workspace: Some(PathBuf::from("/test/workspace")),
                external_id: Some(format!("ext-{}", id)),
                title: Some(format!("Test Conversation {}", id)),
                source_path: PathBuf::from(format!("/test/path/{}.jsonl", id)),
                started_at: Some(1704067200 + id),
                ended_at: None,
                approx_tokens: Some(1000),
                metadata_json: serde_json::json!({"test": true}),
                messages: Vec::new(),
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1704067200),
                content: "Test message".to_string(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            }],
            workspace: Some(Workspace {
                id: Some(1),
                path: PathBuf::from("/test/workspace"),
                display_name: None,
            }),
        }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Insert into cache
        let arc = cache.insert(source_path, view.clone());
        assert_eq!(arc.convo.id, Some(1));

        // Get from cache
        let cached = cache.get(source_path);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().convo.id, Some(1));

        // Check stats
        let (hits, misses, _) = cache.stats().get();
        assert_eq!(hits, 1);
        assert_eq!(misses, 0);
    }

    #[test]
    fn test_cache_miss() {
        let cache = ConversationCache::new(10);

        // Get from empty cache
        let cached = cache.get("/nonexistent/path.jsonl");
        assert!(cached.is_none());

        // Check stats
        let (hits, misses, _) = cache.stats().get();
        assert_eq!(hits, 0);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Insert and verify
        cache.insert(source_path, view);
        assert!(cache.get(source_path).is_some());

        // Invalidate
        cache.invalidate(source_path);
        assert!(cache.get(source_path).is_none());
    }

    #[test]
    fn test_cache_invalidate_all() {
        let cache = ConversationCache::new(10);

        // Insert multiple entries
        for i in 0..5 {
            let view = make_test_view(i);
            let source_path = format!("/test/path/{}.jsonl", i);
            cache.insert(&source_path, view);
        }

        assert_eq!(cache.len(), 5);

        // Invalidate all
        cache.invalidate_all();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = ConversationCache::new(2); // 2 per shard, 32 total

        // Insert more entries than a single shard can hold
        // All entries go to same shard by using paths that hash to same shard
        // (in practice, fxhash distributes well, so we insert many entries)
        for i in 0..100 {
            let view = make_test_view(i);
            let source_path = format!("/test/path/{}.jsonl", i);
            cache.insert(&source_path, view);
        }

        // Some early entries should have been evicted
        let (_, _, evictions) = cache.stats().get();
        assert!(evictions > 0, "Expected some evictions with small capacity");
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Initial hit rate is 0
        assert_eq!(cache.stats().hit_rate(), 0.0);

        // Insert and access twice (1 miss on insert lookup, then 2 hits)
        cache.insert(source_path, view);
        let _ = cache.get(source_path);
        let _ = cache.get(source_path);

        // Hit rate should be positive (2 hits / 2 total)
        let hit_rate = cache.stats().hit_rate();
        assert!(
            hit_rate > 0.5,
            "Expected >50% hit rate, got {:.1}%",
            hit_rate * 100.0
        );
    }

    #[test]
    fn test_cache_shard_distribution() {
        let cache = ConversationCache::new(100);

        // Insert 1000 entries
        for i in 0..1000 {
            let view = make_test_view(i);
            let source_path = format!("/various/paths/{}/session.jsonl", i);
            cache.insert(&source_path, view);
        }

        // All entries should be cached
        assert_eq!(cache.len(), 1000);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let cache = Arc::new(ConversationCache::new(100));
        let mut handles = vec![];

        // Spawn writers
        for t in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..250 {
                    let id = t * 250 + i;
                    let view = make_test_view(id);
                    let source_path = format!("/test/path/{}.jsonl", id);
                    cache.insert(&source_path, view);
                }
            }));
        }

        // Spawn readers
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let source_path = format!("/test/path/{}.jsonl", i);
                    let _ = cache.get(&source_path);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify cache is consistent
        let (hits, misses, _) = cache.stats().get();
        assert!(hits + misses > 0, "Expected some cache operations");
    }
}
