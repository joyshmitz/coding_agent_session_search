use coding_agent_search::perf_evidence::{
    PerfArtifactRef, PerfCount, PerfCountPrecision, PerfEvidenceLedger, PerfMachineProfile,
    PerfPhaseKind, PerfPhaseTiming, PerfProofStatus, PerfProofSummary, PerfReplayGate,
    PerfReplayMetric, PerfReplayThresholds, PerfReplayVerdict, PerfSearchSnapshot, PerfWorkload,
    PerfWorkloadKind, read_perf_evidence_ledger, write_perf_evidence_ledger,
};
use std::fs;

fn ledger(run_id: &str, p99_ms: u64, elapsed_ms: u64) -> PerfEvidenceLedger {
    workload_ledger(
        run_id,
        PerfWorkloadKind::Search,
        "saved-artifact-search",
        ["cass", "search", "memory pressure", "--json"],
        p99_ms,
        elapsed_ms,
    )
}

fn workload_ledger<const N: usize>(
    run_id: &str,
    kind: PerfWorkloadKind,
    name: &str,
    command_args: [&str; N],
    p99_ms: u64,
    elapsed_ms: u64,
) -> PerfEvidenceLedger {
    let mut ledger = PerfEvidenceLedger::new(
        run_id,
        PerfWorkload {
            kind,
            name: name.to_string(),
            description: Some("integration fixture for saved perf evidence replay".to_string()),
            command_args: command_args.iter().map(|arg| (*arg).to_string()).collect(),
            input_count: Some(PerfCount {
                value: 10_000,
                precision: PerfCountPrecision::LowerBound,
            }),
        },
        1_780_000_000_000,
    );
    ledger.machine = PerfMachineProfile {
        logical_cpus: Some(64),
        reserved_cores: Some(8),
        available_memory_bytes: Some(256 * 1024 * 1024 * 1024),
        topology_class: Some("single_host_many_core".to_string()),
    };
    ledger.search = Some(PerfSearchSnapshot {
        query_hash: "blake3:integration-fixture".to_string(),
        limit: 20,
        matched_count: Some(PerfCount {
            value: 250,
            precision: PerfCountPrecision::Exact,
        }),
        returned_hits: 20,
        requested_mode: "hybrid".to_string(),
        realized_mode: "hybrid".to_string(),
        fallback_tier: None,
        timed_out: false,
    });
    ledger.phases = vec![
        phase("queue", PerfPhaseKind::Queueing, elapsed_ms, p99_ms, 0),
        phase("service", PerfPhaseKind::Service, elapsed_ms, p99_ms, 1),
        phase("hydrate", PerfPhaseKind::Hydration, elapsed_ms, p99_ms, 2),
        phase("output", PerfPhaseKind::Output, elapsed_ms, p99_ms, 3),
    ];
    ledger.proof = PerfProofSummary {
        status: PerfProofStatus::Passed,
        baseline_artifact: None,
        comparison_artifact: None,
        p99_regression_basis_points: None,
        notes: vec!["integration fixture proof".to_string()],
    };
    ledger.artifacts = vec![PerfArtifactRef {
        label: "fixture-source".to_string(),
        path: "tests/perf_evidence_replay.rs".to_string(),
        kind: "rust-test".to_string(),
        sha256: None,
    }];
    ledger
}

fn phase(
    name: &str,
    kind: PerfPhaseKind,
    total_elapsed_ms: u64,
    total_p99_ms: u64,
    phase_index: usize,
) -> PerfPhaseTiming {
    let elapsed_ms = split_four_ways(total_elapsed_ms)[phase_index];
    let p99_ms = split_four_ways(total_p99_ms)[phase_index];
    PerfPhaseTiming {
        name: name.to_string(),
        kind,
        elapsed_ms,
        p50_ms: Some(p99_ms.saturating_sub(3)),
        p95_ms: Some(p99_ms.saturating_sub(1)),
        p99_ms: Some(p99_ms),
        samples: Some(PerfCount {
            value: 40,
            precision: PerfCountPrecision::Exact,
        }),
    }
}

fn split_four_ways(total: u64) -> [u64; 4] {
    let base = total / 4;
    let remainder = total % 4;
    let mut parts = [base; 4];
    for part in parts.iter_mut().take(remainder as usize) {
        *part += 1;
    }
    parts
}

#[test]
fn replay_harness_writes_reads_and_gates_saved_ledger_artifacts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let baseline_path = tmp.path().join("baseline.json");
    let current_path = tmp.path().join("current.json");
    let baseline = ledger("baseline-run", 41, 83);
    let current = ledger("current-run", 67, 133);

    let baseline_artifact =
        write_perf_evidence_ledger(&baseline, &baseline_path).expect("write baseline");
    let current_artifact =
        write_perf_evidence_ledger(&current, &current_path).expect("write current");

    assert_eq!(baseline_artifact.kind, "json");
    assert!(baseline_artifact.sha256.is_some());
    assert_eq!(current_artifact.kind, "json");
    assert!(current_artifact.sha256.is_some());

    let decoded = read_perf_evidence_ledger(&current_path).expect("read current");
    assert_eq!(decoded.run_id, "current-run");
    assert_eq!(decoded.workload.command_args[0], "cass");

    let gate = PerfReplayGate::new(
        PerfReplayThresholds::try_new(500, 1_000, 500, 1_000).expect("thresholds"),
    );
    let report = gate
        .replay_files(&current_path, Some(&baseline_path))
        .expect("replay saved artifacts");

    assert_eq!(report.verdict, PerfReplayVerdict::Failure);
    assert!(report.should_fail_build());
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.metric == PerfReplayMetric::ComposedP99),
        "{report:#?}"
    );
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.metric == PerfReplayMetric::TotalElapsed),
        "{report:#?}"
    );
    assert!(
        report.logs.iter().any(|event| {
            event.artifact_path.as_deref() == Some(current_path.to_str().unwrap())
                && event.run_id == "current-run"
                && event.command_args == ["cass", "search", "memory pressure", "--json"]
                && event.failure_reason.is_some()
        }),
        "{report:#?}"
    );
}

#[test]
fn representative_query_index_ledgers_are_generated_and_replay_cleanly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fixtures = [
        workload_ledger(
            "fixture-search",
            PerfWorkloadKind::Search,
            "fixture-search",
            ["cass", "search", "lock contention", "--json"],
            32,
            64,
        ),
        workload_ledger(
            "fixture-watch-once",
            PerfWorkloadKind::WatchOnce,
            "fixture-watch-once",
            [
                "cass",
                "index",
                "--watch-once",
                "/sessions/codex.jsonl",
                "--json",
            ],
            48,
            96,
        ),
        workload_ledger(
            "fixture-full-rebuild",
            PerfWorkloadKind::FullRebuild,
            "fixture-full-rebuild",
            ["cass", "index", "--full", "--json"],
            96,
            192,
        ),
    ];
    let gate = PerfReplayGate::new(PerfReplayThresholds::default());

    for fixture in fixtures {
        let path = tmp.path().join(format!("{}.json", fixture.run_id));
        let artifact = write_perf_evidence_ledger(&fixture, &path).expect("write fixture ledger");
        assert_eq!(artifact.kind, "json");
        assert!(artifact.sha256.is_some());

        let decoded = read_perf_evidence_ledger(&path).expect("read fixture ledger");
        assert_eq!(decoded.run_id, fixture.run_id);
        assert_eq!(decoded.workload.kind, fixture.workload.kind);

        let report = gate
            .replay_files(&path, None)
            .expect("replay fixture ledger without baseline");
        assert_eq!(report.verdict, PerfReplayVerdict::Clean, "{report:#?}");
        assert!(report.logs.iter().any(|event| {
            event.artifact_path.as_deref() == Some(path.to_str().unwrap())
                && event.run_id == fixture.run_id
                && event.command_args == fixture.workload.command_args
        }));
    }
}

#[test]
fn replay_harness_rejects_missing_field_artifact() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let missing_path = tmp.path().join("missing-run-id.json");
    fs::write(
        &missing_path,
        r#"{
  "schema_version": "1",
  "recorded_at_ms": 1,
  "workload": {
    "kind": "search",
    "name": "missing-run-id"
  }
}"#,
    )
    .expect("write malformed fixture");

    let err = read_perf_evidence_ledger(&missing_path)
        .expect_err("missing run_id should reject artifact")
        .to_string();

    assert!(err.contains("missing field `run_id`"), "{err}");
}
