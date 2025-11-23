use coding_agent_search::connectors::{NormalizedConversation, NormalizedMessage};
use coding_agent_search::indexer::persist::persist_conversation;
use coding_agent_search::search::query::SearchClient;
use coding_agent_search::search::tantivy::index_dir;
use coding_agent_search::storage::sqlite::SqliteStorage;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::path::PathBuf;
use tempfile::TempDir;

fn sample_conv(i: i64, msgs: i64) -> NormalizedConversation {
    let mut messages = Vec::new();
    for m in 0..msgs {
        messages.push(NormalizedMessage {
            idx: m,
            role: if m % 2 == 0 { "user" } else { "agent" }.into(),
            author: None,
            created_at: Some(1_700_000_000_000 + (i * 10 + m)),
            content: format!("conversation {i} message {m} lorem ipsum dolor sit amet"),
            extra: serde_json::json!({}),
            snippets: Vec::new(),
        });
    }
    NormalizedConversation {
        agent_slug: "bench-agent".into(),
        external_id: Some(format!("conv-{i}")),
        title: Some(format!("Conversation {i}")),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/bench/conv-{i}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000 + msgs),
        metadata: serde_json::json!({ "bench": true, "i": i }),
        messages,
    }
}

fn seed_index(conv_count: i64, msgs: i64) -> (TempDir, SearchClient) {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_path_buf();
    let db_path = data_dir.join("bench.db");
    let index_path = index_dir(&data_dir).expect("index path");

    let mut storage = SqliteStorage::open(&db_path).expect("open db");
    let mut t_index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path).unwrap();

    for i in 0..conv_count {
        let conv = sample_conv(i, msgs);
        persist_conversation(&mut storage, &mut t_index, &conv).expect("persist");
    }
    t_index.commit().unwrap();

    let client = SearchClient::open(&index_path, Some(&db_path))
        .expect("open client")
        .expect("client available");

    (temp, client)
}

fn bench_indexing(c: &mut Criterion) {
    c.bench_function("index_small_batch", |b| {
        b.iter_batched(
            || {
                let temp = TempDir::new().unwrap();
                let data_dir = temp.path().to_path_buf();
                let db_path = data_dir.join("bench.db");
                let index_path = index_dir(&data_dir).unwrap();
                (
                    temp,
                    SqliteStorage::open(&db_path).unwrap(),
                    coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)
                        .unwrap(),
                )
            },
            |(temp, mut storage, mut idx)| {
                let _keep = temp; // keep tempdir alive
                for i in 0..10 {
                    let conv = sample_conv(i, 10);
                    persist_conversation(&mut storage, &mut idx, &conv).unwrap();
                }
                idx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_search(c: &mut Criterion) {
    let (_tmp, client) = seed_index(40, 12);
    c.bench_function("search_latency", |b| {
        b.iter(|| {
            let hits = client
                .search(
                    black_box("lorem"),
                    coding_agent_search::search::query::SearchFilters::default(),
                    20,
                    0,
                )
                .unwrap();
            black_box(hits.len());
        })
    });
}

criterion_group!(runtime_perf, bench_indexing, bench_search);
criterion_main!(runtime_perf);
