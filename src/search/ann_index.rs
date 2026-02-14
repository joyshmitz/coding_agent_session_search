//! HNSW-based Approximate Nearest Neighbor (ANN) index for semantic search.
//!
//! This module provides O(log n) approximate vector search using the HNSW algorithm,
//! as an alternative to the O(n) exact search in [`crate::search::vector_index`].
//!
//! ## Usage
//!
//! Build HNSW index during indexing:
//! ```bash
//! cass index --semantic --build-hnsw
//! ```
//!
//! Use ANN search at query time:
//! ```bash
//! cass search "query" --mode semantic --approximate
//! ```
//!
//! ## Trade-offs
//!
//! - **Speed**: O(log n) vs O(n) for exact search
//! - **Recall**: ~95-99% depending on ef parameter (configurable)
//! - **Memory**: Additional ~50-100 bytes per vector for graph structure
//! - **Build time**: ~2-5x slower than CVVI-only indexing
//!
//! ## Implementation Notes
//!
//! Uses hnsw_rs with these parameters (from bead coding_agent_session_search-06kc):
//! - M (max_nb_connection): 16 (balances memory/quality)
//! - ef_construction: 200 (good build-time accuracy)
//! - Default ef_search: 100 (tunable at query time)

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hnsw_rs::api::AnnT;
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::hnswio::{HnswIo, ReloadOptions};
use hnsw_rs::prelude::{DistDot, Neighbour};
use ouroboros::self_referencing;

use crate::search::vector_index::{VECTOR_INDEX_DIR, VectorIndex};

/// Magic bytes for HNSW index file format.
pub const HNSW_MAGIC: [u8; 4] = *b"CHSW";

/// HNSW index file version.
pub const HNSW_VERSION: u16 = 1;

/// Default HNSW parameters (from bead recommendations).
pub const DEFAULT_M: usize = 16;
pub const DEFAULT_EF_CONSTRUCTION: usize = 200;
pub const DEFAULT_EF_SEARCH: usize = 100;
pub const DEFAULT_MAX_LAYER: usize = 16;

/// Path to HNSW index file for a given embedder.
pub fn hnsw_index_path(data_dir: &Path, embedder_id: &str) -> PathBuf {
    data_dir
        .join(VECTOR_INDEX_DIR)
        .join(format!("hnsw-{embedder_id}.chsw"))
}

/// Result from an approximate nearest neighbor search.
#[derive(Debug, Clone)]
pub struct AnnSearchResult {
    /// Index into the VectorIndex rows array.
    pub row_idx: usize,
    /// Approximate distance (lower is better for dot product converted to distance).
    pub distance: f32,
}

/// Statistics from an ANN search operation.
///
/// These metrics help users understand the quality/speed tradeoff of approximate search.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct AnnSearchStats {
    /// Total vectors in the HNSW index.
    pub index_size: usize,
    /// Dimension of vectors.
    pub dimension: usize,
    /// ef parameter used for this search (higher = more accurate but slower).
    pub ef_search: usize,
    /// Number of results requested (k).
    pub k_requested: usize,
    /// Number of results returned.
    pub k_returned: usize,
    /// Search time in microseconds.
    pub search_time_us: u64,
    /// Estimated recall based on ef/k ratio.
    /// Formula: min(1.0, 0.9 + 0.1 * log2(ef / k))
    /// This is an empirical estimate; actual recall depends on data distribution.
    pub estimated_recall: f32,
    /// Whether this was an approximate (HNSW) or exact search.
    pub is_approximate: bool,
}

/// HNSW index wrapper for approximate nearest neighbor search.
///
/// The index stores references to row indices in the corresponding VectorIndex,
/// allowing fast approximate lookup followed by metadata retrieval.
#[self_referencing]
struct ReloadedHnsw {
    reloader: HnswIo,
    #[borrows(mut reloader)]
    #[not_covariant]
    hnsw: Hnsw<'this, f32, DistDot>,
}

enum HnswStorage {
    Built(Hnsw<'static, f32, DistDot>),
    Reloaded(ReloadedHnsw),
}

pub struct HnswIndex {
    /// The underlying HNSW graph structure.
    /// Uses DistDot for dot product similarity (converted to distance).
    hnsw: HnswStorage,
    /// Number of vectors in the index.
    count: usize,
    /// Embedder ID this index was built for.
    embedder_id: String,
    /// Dimension of vectors.
    dimension: usize,
}

impl HnswIndex {
    fn with_hnsw<R>(&self, f: impl for<'a> Fn(&Hnsw<'a, f32, DistDot>) -> R) -> R {
        match &self.hnsw {
            HnswStorage::Built(hnsw) => f(hnsw),
            HnswStorage::Reloaded(reloaded) => reloaded.with_hnsw(f),
        }
    }

    /// Build a new HNSW index from an existing VectorIndex.
    ///
    /// This reads all vectors from the CVVI file and builds the HNSW graph.
    /// The row index (position in VectorIndex.rows()) is used as the ID.
    pub fn build_from_vector_index(
        vector_index: &VectorIndex,
        m: usize,
        ef_construction: usize,
    ) -> Result<Self> {
        let count = vector_index.rows().len();
        let dimension = usize::try_from(vector_index.header().dimension)
            .context("Vector index dimension exceeds platform usize")?;
        let embedder_id = vector_index.header().embedder_id.clone();

        if count == 0 {
            bail!("cannot build HNSW index from empty VectorIndex");
        }

        tracing::info!(count, dimension, m, ef_construction, "Building HNSW index");

        // Create HNSW with dot product distance.
        // DistDot computes 1 - dot_product, so lower distance = higher similarity.
        let hnsw: Hnsw<f32, DistDot> =
            Hnsw::new(m, count, DEFAULT_MAX_LAYER, ef_construction, DistDot);

        // Insert all vectors with their row index as ID.
        // Collect vectors first so they stay alive during parallel insertion.
        let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(count);
        for row in vector_index.rows() {
            vectors.push(vector_index.vector_at_f32(row)?);
        }
        let vectors_with_ids: Vec<(&Vec<f32>, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(idx, vec)| (vec, idx))
            .collect();

        // Parallel insertion (HNSW clones vector data internally).
        hnsw.parallel_insert(&vectors_with_ids);

        tracing::info!(count, "HNSW index built successfully");

        Ok(Self {
            hnsw: HnswStorage::Built(hnsw),
            count,
            embedder_id,
            dimension,
        })
    }

    /// Search for approximate nearest neighbors.
    ///
    /// Returns up to `k` results sorted by similarity (highest first).
    /// The `ef` parameter controls search accuracy (higher = more accurate but slower).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<AnnSearchResult>> {
        let (results, _stats) = self.search_with_stats(query, k, ef)?;
        Ok(results)
    }

    /// Search for approximate nearest neighbors with detailed statistics.
    ///
    /// Returns both results and metrics about the search operation.
    pub fn search_with_stats(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<(Vec<AnnSearchResult>, AnnSearchStats)> {
        if query.len() != self.dimension {
            bail!(
                "query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            );
        }

        if k == 0 {
            return Ok((
                Vec::new(),
                AnnSearchStats {
                    index_size: self.count,
                    dimension: self.dimension,
                    ef_search: ef,
                    k_requested: k,
                    k_returned: 0,
                    search_time_us: 0,
                    estimated_recall: 1.0,
                    is_approximate: true,
                },
            ));
        }

        let start = std::time::Instant::now();

        // HNSW search returns neighbors sorted by distance (ascending).
        let neighbors: Vec<Neighbour> = self.with_hnsw(|hnsw| hnsw.search(query, k, ef));

        let search_time_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        // Convert to our result type.
        // DistDot uses 1 - dot_product, so lower distance = higher similarity.
        let results: Vec<AnnSearchResult> = neighbors
            .into_iter()
            .map(|n| AnnSearchResult {
                row_idx: n.d_id,
                distance: n.distance,
            })
            .collect();

        let stats = AnnSearchStats {
            index_size: self.count,
            dimension: self.dimension,
            ef_search: ef,
            k_requested: k,
            k_returned: results.len(),
            search_time_us,
            estimated_recall: estimate_recall(ef, k),
            is_approximate: true,
        };

        Ok((results, stats))
    }

    /// Get the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the embedder ID this index was built for.
    pub fn embedder_id(&self) -> &str {
        &self.embedder_id
    }

    /// Get the vector dimension.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Save the HNSW index to a file.
    ///
    /// Format:
    /// - Magic: "CHSW" (4 bytes)
    /// - Version: u16
    /// - Embedder ID length: u16
    /// - Embedder ID: bytes
    /// - Dimension: u32
    /// - Count: u32
    /// - HNSW graph data (serialized via hnsw_rs)
    pub fn save(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;

        let temp_path = path.with_extension("chsw.tmp");
        let file = File::create(&temp_path)
            .with_context(|| format!("create temp HNSW file {temp_path:?}"))?;
        let mut writer = BufWriter::new(file);

        // Write header.
        writer.write_all(&HNSW_MAGIC)?;
        writer.write_all(&HNSW_VERSION.to_le_bytes())?;

        let id_bytes = self.embedder_id.as_bytes();
        let id_len =
            u16::try_from(id_bytes.len()).map_err(|_| anyhow::anyhow!("embedder_id too long"))?;
        writer.write_all(&id_len.to_le_bytes())?;
        writer.write_all(id_bytes)?;

        let dim_u32 = u32::try_from(self.dimension)
            .map_err(|_| anyhow::anyhow!("dimension {} exceeds u32", self.dimension))?;
        let count_u32 = u32::try_from(self.count)
            .map_err(|_| anyhow::anyhow!("count {} exceeds u32", self.count))?;
        writer.write_all(&dim_u32.to_le_bytes())?;
        writer.write_all(&count_u32.to_le_bytes())?;

        // Serialize HNSW graph using hnsw_rs's file_dump.
        // It creates multiple files: basename.hnsw.graph and basename.hnsw.data
        let temp_dir = parent.join(".hnsw_tmp");
        std::fs::create_dir_all(&temp_dir)?;
        let basename = "hnsw_graph";
        self.with_hnsw(|hnsw| hnsw.file_dump(&temp_dir, basename))
            .with_context(|| "serialize HNSW graph")?;

        // Read the generated files and append to our file.
        let graph_file = temp_dir.join(format!("{basename}.hnsw.graph"));
        let data_file = temp_dir.join(format!("{basename}.hnsw.data"));

        // Read graph file.
        let graph_data = std::fs::read(&graph_file)
            .with_context(|| format!("read HNSW graph {graph_file:?}"))?;
        writer.write_all(&(graph_data.len() as u64).to_le_bytes())?;
        writer.write_all(&graph_data)?;

        // Read data file.
        let data_data =
            std::fs::read(&data_file).with_context(|| format!("read HNSW data {data_file:?}"))?;
        writer.write_all(&(data_data.len() as u64).to_le_bytes())?;
        writer.write_all(&data_data)?;

        writer.flush()?;
        drop(writer);

        // Clean up temp files.
        let _ = std::fs::remove_file(&graph_file);
        let _ = std::fs::remove_file(&data_file);
        let _ = std::fs::remove_dir(&temp_dir);

        // Atomic rename.
        std::fs::rename(&temp_path, path)?;

        tracing::info!(?path, count = self.count, "Saved HNSW index");
        Ok(())
    }

    /// Load an HNSW index from a file.
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open HNSW file {path:?}"))?;
        let mut reader = BufReader::new(file);

        // Read and validate magic.
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != HNSW_MAGIC {
            bail!("invalid HNSW magic: {:?}", magic);
        }

        // Read version.
        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != HNSW_VERSION {
            bail!("unsupported HNSW version: {version}");
        }

        // Read embedder ID.
        let mut id_len_bytes = [0u8; 2];
        reader.read_exact(&mut id_len_bytes)?;
        let id_len = usize::from(u16::from_le_bytes(id_len_bytes));
        let mut id_bytes = vec![0u8; id_len];
        reader.read_exact(&mut id_bytes)?;
        let embedder_id = String::from_utf8(id_bytes)?;

        // Read dimension and count.
        let mut dim_bytes = [0u8; 4];
        reader.read_exact(&mut dim_bytes)?;
        let dimension = usize::try_from(u32::from_le_bytes(dim_bytes))
            .context("HNSW dimension exceeds platform usize")?;

        let mut count_bytes = [0u8; 4];
        reader.read_exact(&mut count_bytes)?;
        let count = usize::try_from(u32::from_le_bytes(count_bytes))
            .context("HNSW count exceeds platform usize")?;

        // Read graph data length.
        let mut graph_len_bytes = [0u8; 8];
        reader.read_exact(&mut graph_len_bytes)?;
        let graph_len = usize::try_from(u64::from_le_bytes(graph_len_bytes))
            .context("HNSW graph data length exceeds platform usize")?;

        // Read graph data to temp file.
        let mut graph_data = vec![0u8; graph_len];
        reader.read_exact(&mut graph_data)?;

        let temp_dir = tempfile::tempdir()?;
        let basename = "hnsw_graph";
        let graph_path = temp_dir.path().join(format!("{basename}.hnsw.graph"));
        let data_path = temp_dir.path().join(format!("{basename}.hnsw.data"));
        std::fs::write(&graph_path, &graph_data)?;

        // Read data length.
        let mut data_len_bytes = [0u8; 8];
        reader.read_exact(&mut data_len_bytes)?;
        let data_len = usize::try_from(u64::from_le_bytes(data_len_bytes))
            .context("HNSW data length exceeds platform usize")?;
        let mut data_data = vec![0u8; data_len];
        reader.read_exact(&mut data_data)?;
        std::fs::write(&data_path, &data_data)?;

        // Load HNSW from the temporary dump files using hnsw_rs loader.
        let mut reloader = HnswIo::new(temp_dir.path(), basename);
        let options = ReloadOptions::default().set_mmap(false);
        debug_assert!(
            !options.use_mmap().0,
            "HNSW mmap MUST be disabled — enabling it would cause use-after-free \
             because temp_dir is dropped at function exit"
        );
        reloader.set_options(options);
        let hnsw = ReloadedHnswTryBuilder {
            reloader,
            hnsw_builder: |reloader| reloader.load_hnsw::<f32, DistDot>(),
        }
        .try_build()?;

        Ok(Self {
            hnsw: HnswStorage::Reloaded(hnsw),
            count,
            embedder_id,
            dimension,
        })
    }

    /// Check if an HNSW index file exists for the given embedder.
    pub fn exists(data_dir: &Path, embedder_id: &str) -> bool {
        hnsw_index_path(data_dir, embedder_id).exists()
    }
}

/// Estimate recall based on ef/k ratio.
///
/// This is an empirical estimate based on HNSW literature:
/// - ef >= k is required for meaningful results
/// - Higher ef/k ratio improves recall
/// - Typical recall is 95-99% for ef/k >= 2
///
/// Formula: min(1.0, 0.85 + 0.15 * min(1.0, log2(ef/k) / 3))
/// This gives:
/// - ef/k = 1: ~85% estimated recall
/// - ef/k = 2: ~90% estimated recall
/// - ef/k = 4: ~95% estimated recall
/// - ef/k = 8+: ~99%+ estimated recall
fn estimate_recall(ef: usize, k: usize) -> f32 {
    if k == 0 {
        return 1.0;
    }
    let ratio = ef as f32 / k as f32;
    if ratio < 1.0 {
        // ef < k is problematic, very low recall expected
        return 0.5 + 0.35 * ratio;
    }
    // log2(ratio) ranges from 0 (ratio=1) to ~3 (ratio=8)
    let log_factor = (ratio.log2() / 3.0).min(1.0);
    (0.85 + 0.15 * log_factor).min(1.0)
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndex")
            .field("count", &self.count)
            .field("embedder_id", &self.embedder_id)
            .field("dimension", &self.dimension)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_path() {
        let path = hnsw_index_path(Path::new("/data"), "fastembed");
        assert_eq!(
            path,
            PathBuf::from("/data/vector_index/hnsw-fastembed.chsw")
        );
    }

    #[test]
    fn test_hnsw_path_with_embedder_id() {
        // Test different embedder IDs
        let path1 = hnsw_index_path(Path::new("/test"), "bge-small");
        assert_eq!(
            path1,
            PathBuf::from("/test/vector_index/hnsw-bge-small.chsw")
        );

        let path2 = hnsw_index_path(Path::new("."), "openai-ada-002");
        assert_eq!(
            path2,
            PathBuf::from("./vector_index/hnsw-openai-ada-002.chsw")
        );
    }

    #[test]
    fn test_estimate_recall_k_zero() {
        // When k=0, recall should be 1.0 (no vectors needed, trivially satisfied)
        assert_eq!(estimate_recall(100, 0), 1.0);
        assert_eq!(estimate_recall(0, 0), 1.0);
    }

    #[test]
    fn test_estimate_recall_ef_less_than_k() {
        // When ef < k, recall is low
        let recall = estimate_recall(5, 10);
        assert!(
            recall < 0.85,
            "recall should be < 0.85 when ef < k, got {recall}"
        );
        assert!(recall > 0.5, "recall should be > 0.5, got {recall}");
    }

    #[test]
    fn test_estimate_recall_ef_equals_k() {
        // ef = k gives approximately 85% recall
        let recall = estimate_recall(10, 10);
        assert!(
            (recall - 0.85).abs() < 0.01,
            "recall should be ~0.85 when ef=k, got {recall}"
        );
    }

    #[test]
    fn test_estimate_recall_ef_double_k() {
        // ef = 2k gives approximately 90% recall
        let recall = estimate_recall(20, 10);
        assert!(
            (0.88..=0.92).contains(&recall),
            "recall should be ~0.90 when ef=2k, got {recall}"
        );
    }

    #[test]
    fn test_estimate_recall_ef_quadruple_k() {
        // ef = 4k gives approximately 95% recall
        let recall = estimate_recall(40, 10);
        assert!(
            (0.93..=0.97).contains(&recall),
            "recall should be ~0.95 when ef=4k, got {recall}"
        );
    }

    #[test]
    fn test_estimate_recall_high_ef() {
        // High ef/k ratio should approach 1.0 but never exceed it
        let recall = estimate_recall(1000, 10);
        assert!(recall <= 1.0, "recall should not exceed 1.0, got {recall}");
        assert!(
            recall >= 0.98,
            "recall should be >= 0.98 with high ef, got {recall}"
        );
    }

    #[test]
    fn test_estimate_recall_default_params() {
        // With default params: ef=100, typical k=10
        let recall = estimate_recall(DEFAULT_EF_SEARCH, 10);
        assert!(
            recall >= 0.95,
            "default params should give >= 95% recall, got {recall}"
        );
    }

    #[test]
    fn test_ann_search_stats_default() {
        let stats = AnnSearchStats::default();
        assert_eq!(stats.index_size, 0);
        assert_eq!(stats.dimension, 0);
        assert_eq!(stats.ef_search, 0);
        assert_eq!(stats.k_requested, 0);
        assert_eq!(stats.k_returned, 0);
        assert_eq!(stats.search_time_us, 0);
        assert!(!stats.is_approximate);
    }

    #[test]
    fn test_ann_search_stats_serialization() {
        let stats = AnnSearchStats {
            index_size: 1000,
            dimension: 384,
            ef_search: 100,
            k_requested: 10,
            k_returned: 10,
            search_time_us: 1234,
            estimated_recall: 0.95,
            is_approximate: true,
        };

        let json = serde_json::to_string(&stats).expect("serialize stats");
        assert!(json.contains("\"index_size\":1000"));
        assert!(json.contains("\"dimension\":384"));
        assert!(json.contains("\"ef_search\":100"));
        assert!(json.contains("\"estimated_recall\":0.95"));
        assert!(json.contains("\"is_approximate\":true"));
    }

    #[test]
    fn test_ann_search_result_fields() {
        let result = AnnSearchResult {
            row_idx: 42,
            distance: 0.123,
        };
        assert_eq!(result.row_idx, 42);
        assert!((result.distance - 0.123).abs() < 0.001);
    }

    #[test]
    fn test_hnsw_magic_and_version() {
        assert_eq!(&HNSW_MAGIC, b"CHSW");
        assert_eq!(HNSW_VERSION, 1);
    }

    #[test]
    fn test_default_parameters() {
        assert_eq!(DEFAULT_M, 16);
        assert_eq!(DEFAULT_EF_CONSTRUCTION, 200);
        assert_eq!(DEFAULT_EF_SEARCH, 100);
        assert_eq!(DEFAULT_MAX_LAYER, 16);
    }

    #[test]
    fn test_hnsw_index_exists_returns_false_for_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(!HnswIndex::exists(temp_dir.path(), "nonexistent"));
    }

    /// Build a small VectorIndex, construct HNSW from it, save to disk,
    /// reload (exercises the ouroboros `ReloadedHnsw` path), and verify
    /// search results match the original built index.
    #[test]
    fn hnsw_build_save_load_roundtrip() {
        use crate::search::vector_index::{Quantization, VectorEntry, VectorIndex};

        // Create 5 orthogonal-ish 4-D vectors so nearest-neighbor is deterministic.
        let entries: Vec<VectorEntry> = (0..5)
            .map(|i| {
                let mut v = vec![0.0_f32; 4];
                v[i % 4] = 1.0;
                // Give the 5th vector a diagonal component so it's distinct.
                if i == 4 {
                    v = vec![0.5, 0.5, 0.5, 0.5];
                }
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: (i as i64) * 1000,
                    agent_id: 1,
                    workspace_id: 10,
                    source_id: 100,
                    role: 0,
                    chunk_idx: 0,
                    content_hash: [i as u8; 32],
                    vector: v,
                }
            })
            .collect();

        let vi = VectorIndex::build("test-embed", "v1", 4, Quantization::F32, entries)
            .expect("build VectorIndex");

        // Build HNSW from the VectorIndex.
        let hnsw = HnswIndex::build_from_vector_index(&vi, DEFAULT_M, DEFAULT_EF_CONSTRUCTION)
            .expect("build HNSW");
        assert_eq!(hnsw.len(), 5);
        assert_eq!(hnsw.dimension(), 4);
        assert_eq!(hnsw.embedder_id(), "test-embed");

        // Search the built index: query [1, 0, 0, 0] should find row 0 first.
        let results_built = hnsw
            .search(&[1.0, 0.0, 0.0, 0.0], 3, DEFAULT_EF_SEARCH)
            .expect("search built");
        assert!(
            !results_built.is_empty(),
            "built search should return results"
        );
        assert_eq!(results_built[0].row_idx, 0, "closest to [1,0,0,0] is row 0");

        // Save to disk.
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("test.chsw");
        hnsw.save(&path).expect("save HNSW");
        assert!(path.exists(), "saved file should exist");

        // Load from disk (exercises ReloadedHnsw / ouroboros path).
        let loaded = HnswIndex::load(&path).expect("load HNSW");
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded.dimension(), 4);
        assert_eq!(loaded.embedder_id(), "test-embed");

        // Search the loaded index and compare results.
        let results_loaded = loaded
            .search(&[1.0, 0.0, 0.0, 0.0], 3, DEFAULT_EF_SEARCH)
            .expect("search loaded");
        assert_eq!(
            results_loaded.len(),
            results_built.len(),
            "loaded and built should return same count"
        );
        assert_eq!(
            results_loaded[0].row_idx, results_built[0].row_idx,
            "loaded and built should agree on top result"
        );

        // Verify search_with_stats on the loaded index.
        let (stats_results, stats) = loaded
            .search_with_stats(&[0.0, 0.0, 0.0, 1.0], 2, DEFAULT_EF_SEARCH)
            .expect("search_with_stats");
        assert!(stats.is_approximate);
        assert_eq!(stats.index_size, 5);
        assert_eq!(stats.dimension, 4);
        assert_eq!(stats.k_requested, 2);
        assert_eq!(stats.k_returned, stats_results.len());
        assert!(
            stats.search_time_us < 10_000_000,
            "search shouldn't take > 10s"
        );
        // query [0,0,0,1] should find row 3 first.
        assert_eq!(stats_results[0].row_idx, 3);
    }

    /// Verify search rejects queries with wrong dimension.
    #[test]
    fn hnsw_search_rejects_dimension_mismatch() {
        use crate::search::vector_index::{Quantization, VectorEntry, VectorIndex};

        let entries = vec![VectorEntry {
            message_id: 1,
            created_at_ms: 1000,
            agent_id: 1,
            workspace_id: 10,
            source_id: 100,
            role: 0,
            chunk_idx: 0,
            content_hash: [0xAA; 32],
            vector: vec![1.0, 0.0, 0.0],
        }];
        let vi = VectorIndex::build("test", "v1", 3, Quantization::F32, entries)
            .expect("build VectorIndex");
        let hnsw = HnswIndex::build_from_vector_index(&vi, DEFAULT_M, DEFAULT_EF_CONSTRUCTION)
            .expect("build HNSW");

        // Wrong dimension query (4 instead of 3).
        let err = hnsw
            .search(&[1.0, 0.0, 0.0, 0.0], 1, DEFAULT_EF_SEARCH)
            .expect_err("should reject dim mismatch");
        assert!(
            err.to_string().contains("dimension mismatch"),
            "error should mention dimension: {err}"
        );
    }

    /// Verify search with k=0 returns empty results without error.
    #[test]
    fn hnsw_search_k_zero_returns_empty() {
        use crate::search::vector_index::{Quantization, VectorEntry, VectorIndex};

        let entries = vec![VectorEntry {
            message_id: 1,
            created_at_ms: 1000,
            agent_id: 1,
            workspace_id: 10,
            source_id: 100,
            role: 0,
            chunk_idx: 0,
            content_hash: [0xBB; 32],
            vector: vec![1.0, 0.0],
        }];
        let vi = VectorIndex::build("test", "v1", 2, Quantization::F32, entries)
            .expect("build VectorIndex");
        let hnsw = HnswIndex::build_from_vector_index(&vi, DEFAULT_M, DEFAULT_EF_CONSTRUCTION)
            .expect("build HNSW");

        let (results, stats) = hnsw
            .search_with_stats(&[1.0, 0.0], 0, DEFAULT_EF_SEARCH)
            .expect("k=0 should succeed");
        assert!(results.is_empty());
        assert_eq!(stats.k_requested, 0);
        assert_eq!(stats.k_returned, 0);
        assert_eq!(stats.estimated_recall, 1.0);
    }
}
