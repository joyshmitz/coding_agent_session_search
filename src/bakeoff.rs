use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Minimal validation report for bake-off runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub model_id: String,
    pub corpus_hash: String,
    pub ndcg_at_10: f64,
    pub latency_ms_p50: u64,
    pub latency_ms_p95: u64,
    pub eligible: bool,
    pub warnings: Vec<String>,
}

/// Compute NDCG@k for a list of relevances in rank order.
/// Non-finite or <= 0 relevances are treated as non-relevant.
pub fn ndcg_at_k(relevances: &[f64], k: usize) -> f64 {
    if k == 0 || relevances.is_empty() {
        return 0.0;
    }
    let dcg = dcg_at_k(relevances, k);
    if dcg == 0.0 {
        return 0.0;
    }
    let mut ideal: Vec<f64> = relevances
        .iter()
        .map(|rel| if rel.is_finite() { rel.max(0.0) } else { 0.0 })
        .collect();
    ideal.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    let idcg = dcg_at_k(&ideal, k);
    if idcg == 0.0 { 0.0 } else { dcg / idcg }
}

fn dcg_at_k(relevances: &[f64], k: usize) -> f64 {
    relevances
        .iter()
        .take(k)
        .enumerate()
        .map(|(idx, rel)| {
            let rel = if rel.is_finite() { *rel } else { 0.0 };
            let rel = rel.max(0.0);
            let denom = (idx as f64 + 2.0).log2();
            (2.0_f64.powf(rel) - 1.0) / denom
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndcg_perfect_is_one() {
        let relevances = vec![3.0, 2.0, 1.0];
        let ndcg = ndcg_at_k(&relevances, 3);
        assert!((ndcg - 1.0).abs() < 1e-9);
    }

    #[test]
    fn ndcg_zero_when_no_relevance() {
        let relevances = vec![0.0, 0.0, 0.0];
        let ndcg = ndcg_at_k(&relevances, 3);
        assert_eq!(ndcg, 0.0);
    }

    #[test]
    fn ndcg_handles_partial_relevance() {
        let relevances = vec![1.0, 0.0, 2.0];
        let ndcg = ndcg_at_k(&relevances, 3);
        assert!(ndcg > 0.0 && ndcg < 1.0);
    }

    #[test]
    fn report_roundtrip() {
        let report = ValidationReport {
            model_id: "hash".to_string(),
            corpus_hash: "deadbeef".to_string(),
            ndcg_at_10: 0.42,
            latency_ms_p50: 12,
            latency_ms_p95: 30,
            eligible: true,
            warnings: vec!["example warning".to_string()],
        };
        let encoded = serde_json::to_string(&report).expect("serialize");
        let decoded: ValidationReport = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(report, decoded);
    }
}
