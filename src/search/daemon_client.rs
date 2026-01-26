//! Daemon client integration for warm embedding and reranking.
//!
//! This module provides:
//! - A `DaemonClient` trait to abstract the daemon protocol (bd-1lps, bd-31z).
//! - Fallback wrappers for `Embedder` and `Reranker` with retry + jittered backoff.
//! - Structured logging for daemon usage and fallback decisions.
//!
//! The concrete daemon transport is intentionally unspecified here until the
//! xf daemon protocol/spec lands. This keeps the integration safe and testable
//! without locking in a protocol prematurely.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{debug, warn};

use crate::search::embedder::{Embedder, EmbedderResult};
use crate::search::reranker::{Reranker, RerankerError, RerankerResult};

/// Retry/backoff configuration for daemon requests.
#[derive(Debug, Clone)]
pub struct DaemonRetryConfig {
    /// Max attempts per request (including the first try).
    pub max_attempts: u32,
    /// Base backoff delay for the first failure.
    pub base_delay: Duration,
    /// Maximum backoff delay.
    pub max_delay: Duration,
    /// Jitter percentage applied to backoff (0.0..=1.0).
    pub jitter_pct: f64,
}

impl Default for DaemonRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 2,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
            jitter_pct: 0.2,
        }
    }
}

impl DaemonRetryConfig {
    /// Load retry config from env if present; fall back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(val) = dotenvy::var("CASS_DAEMON_RETRY_MAX")
            && let Ok(parsed) = val.parse::<u32>()
        {
            cfg.max_attempts = parsed.max(1);
        }
        if let Ok(val) = dotenvy::var("CASS_DAEMON_BACKOFF_BASE_MS")
            && let Ok(parsed) = val.parse::<u64>()
        {
            cfg.base_delay = Duration::from_millis(parsed.max(1));
        }
        if let Ok(val) = dotenvy::var("CASS_DAEMON_BACKOFF_MAX_MS")
            && let Ok(parsed) = val.parse::<u64>()
        {
            cfg.max_delay = Duration::from_millis(parsed.max(1));
        }
        if let Ok(val) = dotenvy::var("CASS_DAEMON_JITTER_PCT")
            && let Ok(parsed) = val.parse::<f64>()
        {
            cfg.jitter_pct = parsed.clamp(0.0, 1.0);
        }
        cfg
    }

    fn backoff_for_attempt(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(explicit) = retry_after {
            return explicit.min(self.max_delay);
        }
        let exp = 2u32.saturating_pow(attempt.saturating_sub(1));
        let base = self.base_delay.checked_mul(exp).unwrap_or(self.max_delay);
        apply_jitter(base.min(self.max_delay), self.jitter_pct)
    }
}

#[derive(Debug, Clone)]
pub enum DaemonError {
    Unavailable(String),
    Timeout(String),
    Overloaded {
        retry_after: Option<Duration>,
        message: String,
    },
    Failed(String),
    InvalidInput(String),
}

impl fmt::Display for DaemonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DaemonError::Unavailable(msg) => write!(f, "daemon unavailable: {msg}"),
            DaemonError::Timeout(msg) => write!(f, "daemon timeout: {msg}"),
            DaemonError::Overloaded { message, .. } => write!(f, "daemon overloaded: {message}"),
            DaemonError::Failed(msg) => write!(f, "daemon failed: {msg}"),
            DaemonError::InvalidInput(msg) => write!(f, "daemon invalid input: {msg}"),
        }
    }
}

impl std::error::Error for DaemonError {}

/// Abstract daemon client. The concrete transport is defined once the protocol is known.
pub trait DaemonClient: Send + Sync {
    fn id(&self) -> &str;
    fn is_available(&self) -> bool;

    fn embed(&self, text: &str, request_id: &str) -> Result<Vec<f32>, DaemonError>;
    fn embed_batch(&self, texts: &[&str], request_id: &str) -> Result<Vec<Vec<f32>>, DaemonError>;
    fn rerank(
        &self,
        query: &str,
        documents: &[&str],
        request_id: &str,
    ) -> Result<Vec<f32>, DaemonError>;
}

/// No-op daemon client used when daemon config is missing.
pub struct NoopDaemonClient {
    id: String,
}

impl NoopDaemonClient {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl DaemonClient for NoopDaemonClient {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_available(&self) -> bool {
        false
    }

    fn embed(&self, _text: &str, _request_id: &str) -> Result<Vec<f32>, DaemonError> {
        Err(DaemonError::Unavailable(
            "daemon not configured".to_string(),
        ))
    }

    fn embed_batch(
        &self,
        _texts: &[&str],
        _request_id: &str,
    ) -> Result<Vec<Vec<f32>>, DaemonError> {
        Err(DaemonError::Unavailable(
            "daemon not configured".to_string(),
        ))
    }

    fn rerank(
        &self,
        _query: &str,
        _documents: &[&str],
        _request_id: &str,
    ) -> Result<Vec<f32>, DaemonError> {
        Err(DaemonError::Unavailable(
            "daemon not configured".to_string(),
        ))
    }
}

#[derive(Debug)]
struct DaemonState {
    consecutive_failures: u32,
    next_retry_at: Option<Instant>,
}

impl DaemonState {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            next_retry_at: None,
        }
    }

    fn can_attempt(&self, now: Instant) -> bool {
        self.next_retry_at.is_none_or(|at| now >= at)
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.next_retry_at = None;
    }

    fn record_failure(&mut self, config: &DaemonRetryConfig, err: &DaemonError) -> Duration {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let retry_after = match err {
            DaemonError::Overloaded { retry_after, .. } => *retry_after,
            _ => None,
        };
        let backoff = config.backoff_for_attempt(self.consecutive_failures, retry_after);
        self.next_retry_at = Some(Instant::now() + backoff);
        backoff
    }
}

#[derive(Debug)]
struct DaemonFailure {
    error: DaemonError,
    attempts: u32,
    backoff: bool,
}

/// Embedder wrapper that uses the daemon when available and falls back to a local embedder.
pub struct DaemonFallbackEmbedder {
    daemon: Arc<dyn DaemonClient>,
    fallback: Arc<dyn Embedder>,
    config: DaemonRetryConfig,
    state: Mutex<DaemonState>,
}

impl DaemonFallbackEmbedder {
    pub fn new(
        daemon: Arc<dyn DaemonClient>,
        fallback: Arc<dyn Embedder>,
        config: DaemonRetryConfig,
    ) -> Self {
        Self {
            daemon,
            fallback,
            config,
            state: Mutex::new(DaemonState::new()),
        }
    }

    fn should_retry(err: &DaemonError) -> bool {
        !matches!(
            err,
            DaemonError::InvalidInput(_) | DaemonError::Overloaded { .. }
        )
    }

    fn fallback_reason(err: &DaemonError, backoff_active: bool) -> &'static str {
        if backoff_active {
            return "backoff";
        }
        match err {
            DaemonError::Unavailable(_) => "unavailable",
            DaemonError::Timeout(_) => "timeout",
            DaemonError::Overloaded { .. } => "overloaded",
            DaemonError::Failed(_) => "error",
            DaemonError::InvalidInput(_) => "invalid",
        }
    }

    fn log_fallback(&self, request_id: &str, retries: u32, reason: &str) {
        warn!(
            daemon_id = self.daemon.id(),
            request_id = request_id,
            retry_count = retries,
            fallback_reason = reason,
            "Daemon embed failed; using local embedder"
        );
    }

    fn try_embed(&self, request_id: &str, text: &str) -> Result<Vec<f32>, DaemonFailure> {
        if !self.daemon.is_available() {
            return Err(DaemonFailure {
                error: DaemonError::Unavailable("daemon not available".to_string()),
                attempts: 0,
                backoff: false,
            });
        }
        let now = Instant::now();
        {
            let state = self.state.lock();
            if !state.can_attempt(now) {
                return Err(DaemonFailure {
                    error: DaemonError::Unavailable("backoff active".to_string()),
                    attempts: 0,
                    backoff: true,
                });
            }
        }
        let mut attempts = 0;
        let mut last_err: Option<DaemonError> = None;
        while attempts < self.config.max_attempts {
            attempts += 1;
            debug!(
                daemon_id = self.daemon.id(),
                request_id = request_id,
                attempt = attempts,
                max_attempts = self.config.max_attempts,
                "Attempting daemon embed"
            );
            match self.daemon.embed(text, request_id) {
                Ok(vector) => {
                    self.state.lock().record_success();
                    return Ok(vector);
                }
                Err(err) => {
                    let should_retry = Self::should_retry(&err);
                    let should_backoff = !matches!(err, DaemonError::InvalidInput(_));
                    let backoff = if should_backoff {
                        Some(self.state.lock().record_failure(&self.config, &err))
                    } else {
                        None
                    };
                    let backoff_ms = backoff.map(|delay| delay.as_millis() as u64).unwrap_or(0);
                    debug!(
                        daemon_id = self.daemon.id(),
                        request_id = request_id,
                        attempt = attempts,
                        max_attempts = self.config.max_attempts,
                        backoff_ms = backoff_ms,
                        will_retry = should_retry && attempts < self.config.max_attempts,
                        error = %&err,
                        "Daemon embed failed"
                    );
                    last_err = Some(err);
                    if !should_retry || attempts >= self.config.max_attempts {
                        break;
                    }
                    if let Some(backoff) = backoff {
                        std::thread::sleep(backoff);
                    }
                }
            }
        }
        Err(DaemonFailure {
            error: last_err
                .unwrap_or_else(|| DaemonError::Unavailable("daemon embed failed".to_string())),
            attempts,
            backoff: false,
        })
    }

    fn try_embed_batch(
        &self,
        request_id: &str,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, DaemonFailure> {
        if !self.daemon.is_available() {
            return Err(DaemonFailure {
                error: DaemonError::Unavailable("daemon not available".to_string()),
                attempts: 0,
                backoff: false,
            });
        }
        let now = Instant::now();
        {
            let state = self.state.lock();
            if !state.can_attempt(now) {
                return Err(DaemonFailure {
                    error: DaemonError::Unavailable("backoff active".to_string()),
                    attempts: 0,
                    backoff: true,
                });
            }
        }
        let mut attempts = 0;
        let mut last_err: Option<DaemonError> = None;
        while attempts < self.config.max_attempts {
            attempts += 1;
            debug!(
                daemon_id = self.daemon.id(),
                request_id = request_id,
                attempt = attempts,
                max_attempts = self.config.max_attempts,
                "Attempting daemon embed batch"
            );
            match self.daemon.embed_batch(texts, request_id) {
                Ok(vectors) => {
                    self.state.lock().record_success();
                    return Ok(vectors);
                }
                Err(err) => {
                    let should_retry = Self::should_retry(&err);
                    let should_backoff = !matches!(err, DaemonError::InvalidInput(_));
                    let backoff = if should_backoff {
                        Some(self.state.lock().record_failure(&self.config, &err))
                    } else {
                        None
                    };
                    let backoff_ms = backoff.map(|delay| delay.as_millis() as u64).unwrap_or(0);
                    debug!(
                        daemon_id = self.daemon.id(),
                        request_id = request_id,
                        attempt = attempts,
                        max_attempts = self.config.max_attempts,
                        backoff_ms = backoff_ms,
                        will_retry = should_retry && attempts < self.config.max_attempts,
                        error = %&err,
                        "Daemon embed batch failed"
                    );
                    last_err = Some(err);
                    if !should_retry || attempts >= self.config.max_attempts {
                        break;
                    }
                    if let Some(backoff) = backoff {
                        std::thread::sleep(backoff);
                    }
                }
            }
        }
        Err(DaemonFailure {
            error: last_err
                .unwrap_or_else(|| DaemonError::Unavailable("daemon embed failed".to_string())),
            attempts,
            backoff: false,
        })
    }
}

impl Embedder for DaemonFallbackEmbedder {
    fn embed(&self, text: &str) -> EmbedderResult<Vec<f32>> {
        let request_id = next_request_id();
        match self.try_embed(&request_id, text) {
            Ok(vector) => Ok(vector),
            Err(failure) => {
                let retries = failure.attempts.saturating_sub(1);
                let reason = Self::fallback_reason(&failure.error, failure.backoff);
                self.log_fallback(&request_id, retries, reason);
                self.fallback.embed(text)
            }
        }
    }

    fn embed_batch(&self, texts: &[&str]) -> EmbedderResult<Vec<Vec<f32>>> {
        let request_id = next_request_id();
        match self.try_embed_batch(&request_id, texts) {
            Ok(vectors) => Ok(vectors),
            Err(failure) => {
                let retries = failure.attempts.saturating_sub(1);
                let reason = Self::fallback_reason(&failure.error, failure.backoff);
                self.log_fallback(&request_id, retries, reason);
                self.fallback.embed_batch(texts)
            }
        }
    }

    fn dimension(&self) -> usize {
        self.fallback.dimension()
    }

    fn id(&self) -> &str {
        self.fallback.id()
    }

    fn is_semantic(&self) -> bool {
        self.fallback.is_semantic()
    }
}

/// Reranker wrapper that uses the daemon when available and falls back to a local reranker.
pub struct DaemonFallbackReranker {
    daemon: Arc<dyn DaemonClient>,
    fallback: Option<Arc<dyn Reranker>>,
    config: DaemonRetryConfig,
    state: Mutex<DaemonState>,
}

impl DaemonFallbackReranker {
    pub fn new(
        daemon: Arc<dyn DaemonClient>,
        fallback: Option<Arc<dyn Reranker>>,
        config: DaemonRetryConfig,
    ) -> Self {
        Self {
            daemon,
            fallback,
            config,
            state: Mutex::new(DaemonState::new()),
        }
    }

    fn log_fallback(&self, request_id: &str, retries: u32, reason: &str) {
        warn!(
            daemon_id = self.daemon.id(),
            request_id = request_id,
            retry_count = retries,
            fallback_reason = reason,
            "Daemon rerank failed; using local reranker"
        );
    }

    fn try_rerank(
        &self,
        request_id: &str,
        query: &str,
        documents: &[&str],
    ) -> Result<Vec<f32>, DaemonFailure> {
        if !self.daemon.is_available() {
            return Err(DaemonFailure {
                error: DaemonError::Unavailable("daemon not available".to_string()),
                attempts: 0,
                backoff: false,
            });
        }
        let now = Instant::now();
        {
            let state = self.state.lock();
            if !state.can_attempt(now) {
                return Err(DaemonFailure {
                    error: DaemonError::Unavailable("backoff active".to_string()),
                    attempts: 0,
                    backoff: true,
                });
            }
        }
        let mut attempts = 0;
        let mut last_err: Option<DaemonError> = None;
        while attempts < self.config.max_attempts {
            attempts += 1;
            debug!(
                daemon_id = self.daemon.id(),
                request_id = request_id,
                attempt = attempts,
                max_attempts = self.config.max_attempts,
                "Attempting daemon rerank"
            );
            match self.daemon.rerank(query, documents, request_id) {
                Ok(scores) => {
                    self.state.lock().record_success();
                    return Ok(scores);
                }
                Err(err) => {
                    let should_retry = DaemonFallbackEmbedder::should_retry(&err);
                    let should_backoff = !matches!(err, DaemonError::InvalidInput(_));
                    let backoff = if should_backoff {
                        Some(self.state.lock().record_failure(&self.config, &err))
                    } else {
                        None
                    };
                    let backoff_ms = backoff.map(|delay| delay.as_millis() as u64).unwrap_or(0);
                    debug!(
                        daemon_id = self.daemon.id(),
                        request_id = request_id,
                        attempt = attempts,
                        max_attempts = self.config.max_attempts,
                        backoff_ms = backoff_ms,
                        will_retry = should_retry && attempts < self.config.max_attempts,
                        error = %&err,
                        "Daemon rerank failed"
                    );
                    last_err = Some(err);
                    if !should_retry || attempts >= self.config.max_attempts {
                        break;
                    }
                    if let Some(backoff) = backoff {
                        std::thread::sleep(backoff);
                    }
                }
            }
        }
        Err(DaemonFailure {
            error: last_err
                .unwrap_or_else(|| DaemonError::Unavailable("daemon rerank failed".to_string())),
            attempts,
            backoff: false,
        })
    }
}

impl Reranker for DaemonFallbackReranker {
    fn rerank(&self, query: &str, documents: &[&str]) -> RerankerResult<Vec<f32>> {
        let request_id = next_request_id();
        match self.try_rerank(&request_id, query, documents) {
            Ok(scores) => Ok(scores),
            Err(failure) => {
                let retries = failure.attempts.saturating_sub(1);
                let reason =
                    DaemonFallbackEmbedder::fallback_reason(&failure.error, failure.backoff);
                self.log_fallback(&request_id, retries, reason);
                match &self.fallback {
                    Some(reranker) => reranker.rerank(query, documents),
                    None => Err(RerankerError::Unavailable(
                        "no local reranker available".to_string(),
                    )),
                }
            }
        }
    }

    fn id(&self) -> &str {
        if let Some(fallback) = &self.fallback {
            fallback.id()
        } else {
            "daemon-reranker"
        }
    }

    fn is_available(&self) -> bool {
        self.daemon.is_available()
            || self
                .fallback
                .as_ref()
                .map(|r| r.is_available())
                .unwrap_or(false)
    }
}

fn apply_jitter(duration: Duration, jitter_pct: f64) -> Duration {
    if jitter_pct <= 0.0 {
        return duration;
    }
    let unit = next_jitter_unit();
    let delta = (unit * 2.0 - 1.0) * jitter_pct;
    let base_ms = duration.as_millis() as f64;
    let jittered = (base_ms * (1.0 + delta)).max(1.0);
    Duration::from_millis(jittered.round() as u64)
}

fn next_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("daemon-{id}")
}

fn next_jitter_unit() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    let mut current = SEED.load(Ordering::Relaxed);
    loop {
        let next = current.wrapping_mul(6364136223846793005u64).wrapping_add(1);
        match SEED.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                // Use top 53 bits for a uniform f64 in [0, 1)
                let value = next >> 11;
                return (value as f64) / ((1u64 << 53) as f64);
            }
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::hash_embedder::HashEmbedder;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ALLOWLIST: TestDaemon is a test harness that simulates controlled daemon failures
    // for testing retry/backoff/fallback logic. This cannot be replaced with a "real" daemon
    // because we need deterministic control over failure modes (timeout, overload, invalid input)
    // to verify the retry policy. Integration tests in tests/daemon_client_integration.rs use
    // ChannelDaemonClient for more realistic channel-based communication testing.
    //
    // Classification: (c) ALLOWLIST - Test utility for edge case simulation
    // See: test-results/no_mock_audit.md
    struct TestDaemon {
        calls: AtomicUsize,
        fail_first: usize,
        available: bool,
        mode: FailureMode,
    }

    #[derive(Clone, Copy)]
    enum FailureMode {
        Unavailable,
        Timeout,
        Overloaded { retry_after: Duration },
        Failed,
        InvalidInput,
    }

    impl FailureMode {
        fn error(&self) -> DaemonError {
            match self {
                FailureMode::Unavailable => DaemonError::Unavailable("daemon down".to_string()),
                FailureMode::Timeout => DaemonError::Timeout("daemon timeout".to_string()),
                FailureMode::Overloaded { retry_after } => DaemonError::Overloaded {
                    retry_after: Some(*retry_after),
                    message: "queue full".to_string(),
                },
                FailureMode::Failed => DaemonError::Failed("daemon error".to_string()),
                FailureMode::InvalidInput => DaemonError::InvalidInput("invalid input".to_string()),
            }
        }
    }

    impl TestDaemon {
        fn new(fail_first: usize) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first,
                available: true,
                mode: FailureMode::Unavailable,
            }
        }

        fn new_with_mode(fail_first: usize, mode: FailureMode) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first,
                available: true,
                mode,
            }
        }
    }

    impl DaemonClient for TestDaemon {
        fn id(&self) -> &str {
            "test-daemon"
        }

        fn is_available(&self) -> bool {
            self.available
        }

        fn embed(&self, _text: &str, _request_id: &str) -> Result<Vec<f32>, DaemonError> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call < self.fail_first {
                Err(self.mode.error())
            } else {
                Ok(vec![2.0; 4])
            }
        }

        fn embed_batch(
            &self,
            texts: &[&str],
            _request_id: &str,
        ) -> Result<Vec<Vec<f32>>, DaemonError> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call < self.fail_first {
                Err(self.mode.error())
            } else {
                Ok(texts.iter().map(|_| vec![2.0; 4]).collect())
            }
        }

        fn rerank(
            &self,
            _query: &str,
            documents: &[&str],
            _request_id: &str,
        ) -> Result<Vec<f32>, DaemonError> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call < self.fail_first {
                Err(self.mode.error())
            } else {
                Ok(documents.iter().map(|_| 1.0).collect())
            }
        }
    }

    // Helper constant: TestDaemon returns embeddings with dimension 4
    const TEST_DAEMON_DIM: usize = 4;

    // Helper to create a HashEmbedder with the same dimension as TestDaemon for fallback
    fn test_hash_embedder() -> HashEmbedder {
        HashEmbedder::new(TEST_DAEMON_DIM)
    }

    // Helper to check if a result came from the daemon (all 2.0) vs fallback (variable)
    fn is_daemon_result(result: &[f32]) -> bool {
        result.iter().all(|&v| (v - 2.0).abs() < f32::EPSILON)
    }

    #[test]
    fn daemon_embedder_falls_back_on_failure() {
        let daemon = Arc::new(TestDaemon::new(10)); // fails 10 times
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon, fallback, cfg);
        let result = embedder.embed("hello").unwrap();
        // Should use fallback (HashEmbedder), not daemon
        assert_eq!(result.len(), TEST_DAEMON_DIM);
        assert!(
            !is_daemon_result(&result),
            "expected fallback, got daemon result"
        );
    }

    #[test]
    fn daemon_reranker_falls_back_on_failure() {
        let daemon = Arc::new(TestDaemon::new(10)); // fails 10 times
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            ..DaemonRetryConfig::default()
        };

        // Without fallback, reranker should return error when daemon fails
        let reranker_no_fallback = DaemonFallbackReranker::new(daemon.clone(), None, cfg.clone());
        let result = reranker_no_fallback.rerank("query", &["doc a", "doc b"]);
        assert!(
            result.is_err(),
            "expected error when no fallback and daemon fails"
        );

        // With a daemon that eventually succeeds after retries, it should work
        let working_daemon = Arc::new(TestDaemon::new(0)); // succeeds immediately
        let reranker_working = DaemonFallbackReranker::new(working_daemon, None, cfg);
        let result = reranker_working
            .rerank("query", &["doc a", "doc b"])
            .unwrap();
        assert_eq!(result.len(), 2);
        // TestDaemon returns 1.0 for each document
        assert!((result[0] - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn daemon_embedder_retries_then_succeeds() {
        let daemon = Arc::new(TestDaemon::new(1)); // fails first call only
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let result = embedder.embed("hello").unwrap();
        // Should succeed on second try with daemon result
        assert!(
            is_daemon_result(&result),
            "expected daemon result after retry"
        );
        assert_eq!(daemon.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn daemon_timeout_retries_then_falls_back() {
        let daemon = Arc::new(TestDaemon::new_with_mode(2, FailureMode::Timeout));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon, fallback, cfg);
        let result = embedder.embed("hello").unwrap();
        // Should fall back to HashEmbedder after exhausting retries
        assert!(
            !is_daemon_result(&result),
            "expected fallback after timeout"
        );
    }

    #[test]
    fn daemon_invalid_input_does_not_retry() {
        let daemon = Arc::new(TestDaemon::new_with_mode(1, FailureMode::InvalidInput));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 3,
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let result = embedder.embed("hello").unwrap();
        // InvalidInput should not retry, just fallback immediately
        assert!(
            !is_daemon_result(&result),
            "expected fallback on invalid input"
        );
        assert_eq!(daemon.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn daemon_invalid_input_does_not_backoff() {
        let daemon = Arc::new(TestDaemon::new_with_mode(1, FailureMode::InvalidInput));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(50),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let first = embedder.embed("hello").unwrap();
        // First call fails with InvalidInput, falls back
        assert!(!is_daemon_result(&first), "expected fallback on first call");

        // Second call should try daemon again (no backoff for InvalidInput)
        let second = embedder.embed("hello-again").unwrap();
        assert!(
            is_daemon_result(&second),
            "expected daemon result on second call"
        );
        assert_eq!(daemon.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn daemon_failed_retries_then_falls_back() {
        let daemon = Arc::new(TestDaemon::new_with_mode(2, FailureMode::Failed));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let result = embedder.embed("hello").unwrap();
        assert!(
            !is_daemon_result(&result),
            "expected fallback after failures"
        );
        assert_eq!(daemon.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn daemon_overload_sets_backoff() {
        let daemon = Arc::new(TestDaemon::new_with_mode(
            1,
            FailureMode::Overloaded {
                retry_after: Duration::from_millis(25),
            },
        ));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let _ = embedder.embed("first").unwrap();
        let calls_after_first = daemon.calls.load(Ordering::Relaxed);

        // Second call should skip daemon due to backoff
        let _ = embedder.embed("second").unwrap();
        let calls_after_second = daemon.calls.load(Ordering::Relaxed);
        assert_eq!(calls_after_first, calls_after_second);
    }

    #[test]
    fn daemon_overload_respects_retry_after() {
        let retry_after = Duration::from_millis(40);
        let daemon = Arc::new(TestDaemon::new_with_mode(
            1,
            FailureMode::Overloaded { retry_after },
        ));
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(100),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let _ = embedder.embed("first").unwrap();
        let calls_after_first = daemon.calls.load(Ordering::Relaxed);

        // Before retry_after expires, should skip daemon
        std::thread::sleep(Duration::from_millis(10));
        let _ = embedder.embed("second").unwrap();
        let calls_after_second = daemon.calls.load(Ordering::Relaxed);
        assert_eq!(calls_after_first, calls_after_second);

        // After retry_after expires, should try daemon again
        std::thread::sleep(Duration::from_millis(45));
        let _ = embedder.embed("third").unwrap();
        let calls_after_third = daemon.calls.load(Ordering::Relaxed);
        assert!(calls_after_third > calls_after_second);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let base = Duration::from_millis(100);
        let jitter_pct = 0.2;
        let min_ms = (base.as_millis() as f64 * (1.0 - jitter_pct)) as u64;
        let max_ms = (base.as_millis() as f64 * (1.0 + jitter_pct)) as u64;

        for _ in 0..100 {
            let jittered = apply_jitter(base, jitter_pct);
            let ms = jittered.as_millis() as u64;
            assert!(ms >= min_ms, "jitter too low: {ms} < {min_ms}");
            assert!(ms <= max_ms, "jitter too high: {ms} > {max_ms}");
        }
    }

    #[test]
    fn daemon_backoff_skips_until_ready() {
        let daemon = Arc::new(TestDaemon::new(1)); // fails first call
        let fallback = Arc::new(test_hash_embedder());
        let cfg = DaemonRetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(10),
            ..DaemonRetryConfig::default()
        };

        let embedder = DaemonFallbackEmbedder::new(daemon.clone(), fallback, cfg);
        let _ = embedder.embed("first").unwrap();
        let calls_after_first = daemon.calls.load(Ordering::Relaxed);

        // Immediate retry should be skipped due to backoff
        let _ = embedder.embed("second").unwrap();
        let calls_after_second = daemon.calls.load(Ordering::Relaxed);
        assert_eq!(calls_after_first, calls_after_second);

        // After backoff expires, should try daemon again
        std::thread::sleep(Duration::from_millis(15));
        let _ = embedder.embed("third").unwrap();
        let calls_after_third = daemon.calls.load(Ordering::Relaxed);
        assert!(calls_after_third > calls_after_second);
    }
}
