//! SQL query builders for analytics.
//!
//! All functions accept a `&rusqlite::Connection` and an [`AnalyticsFilter`],
//! keeping the SQL and bucketing logic in one place for both CLI and ftui.

use std::collections::BTreeMap;

use rusqlite::Connection;

use super::bucketing;
use super::types::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check whether a table exists in the database.
pub fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |_| Ok(()),
    )
    .is_ok()
}

/// Internal stats from a single COUNT/MIN/MAX query on a rollup table.
#[derive(Debug, Default)]
struct RollupStats {
    row_count: i64,
    min_day: Option<i64>,
    max_day: Option<i64>,
    last_updated: Option<i64>,
}

/// Query row counts and range for a single analytics table.
fn query_table_stats(
    conn: &Connection,
    table: &str,
    day_col: &str,
    updated_col: Option<&str>,
) -> RollupStats {
    if !table_exists(conn, table) {
        return RollupStats::default();
    }
    let sql = match updated_col {
        Some(uc) => {
            format!("SELECT COUNT(*), MIN({day_col}), MAX({day_col}), MAX({uc}) FROM {table}")
        }
        None => format!("SELECT COUNT(*), MIN({day_col}), MAX({day_col}), NULL FROM {table}"),
    };
    conn.query_row(&sql, [], |row| {
        Ok(RollupStats {
            row_count: row.get::<_, i64>(0).unwrap_or(0),
            min_day: row.get::<_, Option<i64>>(1).unwrap_or(None),
            max_day: row.get::<_, Option<i64>>(2).unwrap_or(None),
            last_updated: row.get::<_, Option<i64>>(3).unwrap_or(None),
        })
    })
    .unwrap_or_default()
}

/// Build SQL WHERE clause fragments and bind-parameter values from an
/// [`AnalyticsFilter`]'s dimensional (non-time) filters.
///
/// Returns `(clause_fragments, param_values)` where each fragment is like
/// `"agent_slug IN (?1, ?2)"` and `param_values` are the corresponding bind
/// strings.
pub fn build_where_parts(filter: &AnalyticsFilter) -> (Vec<String>, Vec<String>) {
    let mut parts = Vec::new();
    let mut params = Vec::new();

    // Agent filters — multiple agents are OR'd together.
    if !filter.agents.is_empty() {
        let placeholders: Vec<String> = filter
            .agents
            .iter()
            .map(|a| {
                params.push(a.clone());
                format!("?{}", params.len())
            })
            .collect();
        parts.push(format!("agent_slug IN ({})", placeholders.join(", ")));
    }

    // Source filter.
    match &filter.source {
        SourceFilter::All => {}
        SourceFilter::Local => {
            params.push("local".into());
            parts.push(format!("source_id = ?{}", params.len()));
        }
        SourceFilter::Remote => {
            params.push("local".into());
            parts.push(format!("source_id != ?{}", params.len()));
        }
        SourceFilter::Specific(s) => {
            params.push(s.clone());
            parts.push(format!("source_id = ?{}", params.len()));
        }
    }

    (parts, params)
}

// ---------------------------------------------------------------------------
// query_status
// ---------------------------------------------------------------------------

/// Run the analytics status query — returns table health, coverage, and drift.
pub fn query_status(conn: &Connection, _filter: &AnalyticsFilter) -> AnalyticsResult<StatusResult> {
    // 1. Check which analytics tables actually exist.
    let has_message_metrics = table_exists(conn, "message_metrics");
    let has_usage_hourly = table_exists(conn, "usage_hourly");
    let has_usage_daily = table_exists(conn, "usage_daily");
    let has_token_usage = table_exists(conn, "token_usage");
    let has_token_daily_stats = table_exists(conn, "token_daily_stats");

    // 2. Gather per-table row counts and coverage range.
    let mm = query_table_stats(conn, "message_metrics", "day_id", None);
    let uh = query_table_stats(conn, "usage_hourly", "hour_id", Some("last_updated"));
    let ud = query_table_stats(conn, "usage_daily", "day_id", Some("last_updated"));
    let tu = query_table_stats(conn, "token_usage", "day_id", None);
    let tds = query_table_stats(conn, "token_daily_stats", "day_id", Some("last_updated"));

    // 3. Coverage diagnostics.
    let total_messages: i64 = conn
        .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
        .unwrap_or(0);

    let api_coverage_pct = if has_message_metrics && mm.row_count > 0 {
        let api_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM message_metrics WHERE api_data_source = 'api'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if mm.row_count > 0 {
            (api_count as f64 / mm.row_count as f64) * 100.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let model_coverage_pct = if has_token_usage && tu.row_count > 0 {
        let with_model: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM token_usage WHERE model_name IS NOT NULL AND model_name != ''",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        (with_model as f64 / tu.row_count as f64) * 100.0
    } else {
        0.0
    };

    let estimate_only_pct = if has_token_usage && tu.row_count > 0 {
        let estimates: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM token_usage WHERE data_source = 'estimated'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        (estimates as f64 / tu.row_count as f64) * 100.0
    } else {
        0.0
    };

    let mm_coverage_pct = if total_messages > 0 {
        (mm.row_count as f64 / total_messages as f64) * 100.0
    } else {
        0.0
    };

    // 4. Drift detection.
    let mut drift_signals: Vec<DriftSignal> = Vec::new();

    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let stale_threshold_secs: i64 = 86400;

    let track_a_fresh = uh
        .last_updated
        .is_some_and(|t| (now_epoch - t).abs() < stale_threshold_secs);
    let track_b_fresh = tds
        .last_updated
        .is_some_and(|t| (now_epoch - t).abs() < stale_threshold_secs);

    if track_a_fresh && !track_b_fresh && has_token_daily_stats {
        drift_signals.push(DriftSignal {
            signal: "track_freshness_mismatch".into(),
            detail:
                "Track A (usage_hourly/daily) is fresh but Track B (token_daily_stats) is stale"
                    .into(),
            severity: "warning".into(),
        });
    }
    if track_b_fresh && !track_a_fresh && has_usage_hourly {
        drift_signals.push(DriftSignal {
            signal: "track_freshness_mismatch".into(),
            detail:
                "Track B (token_daily_stats) is fresh but Track A (usage_hourly/daily) is stale"
                    .into(),
            severity: "warning".into(),
        });
    }

    if mm.row_count > 0 && uh.row_count == 0 && has_usage_hourly {
        drift_signals.push(DriftSignal {
            signal: "missing_rollups".into(),
            detail: "message_metrics has data but usage_hourly is empty — rebuild needed".into(),
            severity: "error".into(),
        });
    }
    if mm.row_count > 0 && ud.row_count == 0 && has_usage_daily {
        drift_signals.push(DriftSignal {
            signal: "missing_rollups".into(),
            detail: "message_metrics has data but usage_daily is empty — rebuild needed".into(),
            severity: "error".into(),
        });
    }
    if tu.row_count > 0 && tds.row_count == 0 && has_token_daily_stats {
        drift_signals.push(DriftSignal {
            signal: "missing_rollups".into(),
            detail: "token_usage has data but token_daily_stats is empty — rebuild needed".into(),
            severity: "error".into(),
        });
    }

    if total_messages > 100 && mm.row_count == 0 && tu.row_count == 0 {
        drift_signals.push(DriftSignal {
            signal: "no_analytics_data".into(),
            detail: format!("{total_messages} messages indexed but no analytics computed"),
            severity: "error".into(),
        });
    }

    // 5. Recommended action.
    let has_error_drift = drift_signals.iter().any(|s| s.severity == "error");
    let has_warning_drift = drift_signals.iter().any(|s| s.severity == "warning");

    let recommended_action = if has_error_drift {
        if mm.row_count == 0 && tu.row_count == 0 {
            "rebuild_all"
        } else if mm.row_count > 0 && (uh.row_count == 0 || ud.row_count == 0) {
            "rebuild_track_a"
        } else if tu.row_count > 0 && tds.row_count == 0 {
            "rebuild_track_b"
        } else {
            "rebuild_all"
        }
    } else if has_warning_drift {
        if track_a_fresh && !track_b_fresh {
            "rebuild_track_b"
        } else if track_b_fresh && !track_a_fresh {
            "rebuild_track_a"
        } else {
            "none"
        }
    } else {
        "none"
    };

    // 6. Assemble result.
    let make_table_info = |name: &str, exists: bool, stats: &RollupStats| TableInfo {
        table: name.into(),
        exists,
        row_count: stats.row_count,
        min_day_id: stats.min_day,
        max_day_id: stats.max_day,
        last_updated: stats.last_updated,
    };

    Ok(StatusResult {
        tables: vec![
            make_table_info("message_metrics", has_message_metrics, &mm),
            make_table_info("usage_hourly", has_usage_hourly, &uh),
            make_table_info("usage_daily", has_usage_daily, &ud),
            make_table_info("token_usage", has_token_usage, &tu),
            make_table_info("token_daily_stats", has_token_daily_stats, &tds),
        ],
        coverage: CoverageInfo {
            total_messages,
            message_metrics_coverage_pct: (mm_coverage_pct * 100.0).round() / 100.0,
            api_token_coverage_pct: (api_coverage_pct * 100.0).round() / 100.0,
            model_name_coverage_pct: (model_coverage_pct * 100.0).round() / 100.0,
            estimate_only_pct: (estimate_only_pct * 100.0).round() / 100.0,
        },
        drift: DriftInfo {
            signals: drift_signals,
            track_a_fresh,
            track_b_fresh,
        },
        recommended_action: recommended_action.into(),
    })
}

// ---------------------------------------------------------------------------
// query_tokens_timeseries
// ---------------------------------------------------------------------------

/// Run the token/usage timeseries query with the given bucketing granularity.
pub fn query_tokens_timeseries(
    conn: &Connection,
    filter: &AnalyticsFilter,
    group_by: GroupBy,
) -> AnalyticsResult<TimeseriesResult> {
    let query_start = std::time::Instant::now();

    // Choose source table and bucket column.
    let (table, bucket_col) = match group_by {
        GroupBy::Hour => ("usage_hourly", "hour_id"),
        _ => ("usage_daily", "day_id"),
    };

    // Check that the source table exists.
    if !table_exists(conn, table) {
        return Ok(TimeseriesResult {
            buckets: vec![],
            totals: UsageBucket::default(),
            source_table: table.into(),
            group_by,
            elapsed_ms: query_start.elapsed().as_millis() as u64,
            path: "none".into(),
        });
    }

    // Build WHERE clause.
    let (day_min, day_max) = bucketing::resolve_day_range(filter);
    let (hour_min, hour_max) = bucketing::resolve_hour_range(filter);

    let (dim_parts, dim_params) = build_where_parts(filter);
    let mut where_parts = dim_parts;
    let mut bind_values = dim_params;

    match group_by {
        GroupBy::Hour => {
            if let Some(min) = hour_min {
                bind_values.push(min.to_string());
                where_parts.push(format!("{bucket_col} >= ?{}", bind_values.len()));
            }
            if let Some(max) = hour_max {
                bind_values.push(max.to_string());
                where_parts.push(format!("{bucket_col} <= ?{}", bind_values.len()));
            }
        }
        _ => {
            if let Some(min) = day_min {
                bind_values.push(min.to_string());
                where_parts.push(format!("{bucket_col} >= ?{}", bind_values.len()));
            }
            if let Some(max) = day_max {
                bind_values.push(max.to_string());
                where_parts.push(format!("{bucket_col} <= ?{}", bind_values.len()));
            }
        }
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };

    let sql = format!(
        "SELECT {bucket_col},
                SUM(message_count),
                SUM(user_message_count),
                SUM(assistant_message_count),
                SUM(tool_call_count),
                SUM(plan_message_count),
                SUM(api_coverage_message_count),
                SUM(content_tokens_est_total),
                SUM(content_tokens_est_user),
                SUM(content_tokens_est_assistant),
                SUM(api_tokens_total),
                SUM(api_input_tokens_total),
                SUM(api_output_tokens_total),
                SUM(api_cache_read_tokens_total),
                SUM(api_cache_creation_tokens_total),
                SUM(api_thinking_tokens_total)
         FROM {table}
         {where_clause}
         GROUP BY {bucket_col}
         ORDER BY {bucket_col}"
    );

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| AnalyticsError::Db(format!("Failed to prepare analytics query: {e}")))?;

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = bind_values
        .iter()
        .map(|v| v as &dyn rusqlite::types::ToSql)
        .collect();

    let rows_result = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, i64>(15)?,
            ))
        })
        .map_err(|e| AnalyticsError::Db(format!("Analytics query failed: {e}")))?;

    let mut raw_buckets: Vec<(i64, UsageBucket)> = Vec::new();
    for row in rows_result {
        let r = row.map_err(|e| AnalyticsError::Db(format!("Row read error: {e}")))?;
        raw_buckets.push((
            r.0,
            UsageBucket {
                message_count: r.1,
                user_message_count: r.2,
                assistant_message_count: r.3,
                tool_call_count: r.4,
                plan_message_count: r.5,
                api_coverage_message_count: r.6,
                content_tokens_est_total: r.7,
                content_tokens_est_user: r.8,
                content_tokens_est_assistant: r.9,
                api_tokens_total: r.10,
                api_input_tokens_total: r.11,
                api_output_tokens_total: r.12,
                api_cache_read_tokens_total: r.13,
                api_cache_creation_tokens_total: r.14,
                api_thinking_tokens_total: r.15,
            },
        ));
    }

    // Re-bucket by week or month if needed.
    let final_buckets: Vec<(String, UsageBucket)> = match group_by {
        GroupBy::Hour => raw_buckets
            .into_iter()
            .map(|(id, row)| (bucketing::hour_id_to_iso(id), row))
            .collect(),
        GroupBy::Day => raw_buckets
            .into_iter()
            .map(|(id, row)| (bucketing::day_id_to_iso(id), row))
            .collect(),
        GroupBy::Week => {
            let mut merged: BTreeMap<String, UsageBucket> = BTreeMap::new();
            for (day_id, row) in raw_buckets {
                let key = bucketing::day_id_to_iso_week(day_id);
                merged.entry(key).or_default().merge(&row);
            }
            merged.into_iter().collect()
        }
        GroupBy::Month => {
            let mut merged: BTreeMap<String, UsageBucket> = BTreeMap::new();
            for (day_id, row) in raw_buckets {
                let key = bucketing::day_id_to_month(day_id);
                merged.entry(key).or_default().merge(&row);
            }
            merged.into_iter().collect()
        }
    };

    // Compute totals.
    let mut totals = UsageBucket::default();
    for (_, row) in &final_buckets {
        totals.merge(row);
    }

    let elapsed_ms = query_start.elapsed().as_millis() as u64;

    Ok(TimeseriesResult {
        buckets: final_buckets,
        totals,
        source_table: table.into(),
        group_by,
        elapsed_ms,
        path: "rollup".into(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_where_parts_empty_filter() {
        let f = AnalyticsFilter::default();
        let (parts, params) = build_where_parts(&f);
        assert!(parts.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn build_where_parts_single_agent() {
        let f = AnalyticsFilter {
            agents: vec!["claude_code".into()],
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].contains("agent_slug IN"));
        assert_eq!(params, vec!["claude_code"]);
    }

    #[test]
    fn build_where_parts_multiple_agents() {
        let f = AnalyticsFilter {
            agents: vec!["claude_code".into(), "codex".into(), "aider".into()],
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].contains("?1"));
        assert!(parts[0].contains("?2"));
        assert!(parts[0].contains("?3"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_where_parts_source_local() {
        let f = AnalyticsFilter {
            source: SourceFilter::Local,
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].contains("source_id = ?1"));
        assert_eq!(params, vec!["local"]);
    }

    #[test]
    fn build_where_parts_source_remote() {
        let f = AnalyticsFilter {
            source: SourceFilter::Remote,
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].contains("source_id != ?1"));
        assert_eq!(params, vec!["local"]);
    }

    #[test]
    fn build_where_parts_source_specific() {
        let f = AnalyticsFilter {
            source: SourceFilter::Specific("myhost.local".into()),
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].contains("source_id = ?1"));
        assert_eq!(params, vec!["myhost.local"]);
    }

    #[test]
    fn build_where_parts_combined() {
        let f = AnalyticsFilter {
            agents: vec!["codex".into()],
            source: SourceFilter::Local,
            ..Default::default()
        };
        let (parts, params) = build_where_parts(&f);
        assert_eq!(parts.len(), 2);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], "codex");
        assert_eq!(params[1], "local");
    }
}
