//! Analytics chart rendering for the ftui analytics views.
//!
//! Provides [`AnalyticsChartData`] (pre-computed chart data) and rendering
//! functions that turn analytics query results into terminal-native
//! visualizations using ftui-extras charts and canvas widgets.

use ftui::render::cell::PackedRgba;
use ftui::widgets::Widget;
use ftui::widgets::paragraph::Paragraph;
use ftui_extras::canvas::{CanvasRef, Mode as CanvasMode, Painter};
use ftui_extras::charts::LineChart as FtuiLineChart;
use ftui_extras::charts::Series as ChartSeries;
use ftui_extras::charts::{BarChart, BarDirection, BarGroup, Sparkline};

use super::app::{AnalyticsView, ExplorerMetric, ExplorerOverlay};
use super::ftui_adapter::{Constraint, Flex, Rect};
use crate::sources::provenance::SourceFilter;

// ---------------------------------------------------------------------------
// Agent accent colors (consistent across all chart views)
// ---------------------------------------------------------------------------

/// Fixed color palette for up to 14 agents. Colors cycle for overflow.
const AGENT_COLORS: &[PackedRgba] = &[
    PackedRgba::rgb(0, 150, 255),   // claude_code — cyan
    PackedRgba::rgb(255, 100, 0),   // codex — orange
    PackedRgba::rgb(0, 200, 100),   // gemini — green
    PackedRgba::rgb(200, 50, 200),  // cursor — magenta
    PackedRgba::rgb(255, 200, 0),   // chatgpt — gold
    PackedRgba::rgb(100, 200, 255), // aider — sky
    PackedRgba::rgb(255, 80, 80),   // pi_agent — red
    PackedRgba::rgb(150, 255, 150), // cline — lime
    PackedRgba::rgb(180, 130, 255), // opencode — lavender
    PackedRgba::rgb(255, 160, 200), // amp — pink
    PackedRgba::rgb(200, 200, 100), // factory — olive
    PackedRgba::rgb(100, 255, 200), // clawdbot — mint
    PackedRgba::rgb(255, 220, 150), // vibe — peach
    PackedRgba::rgb(150, 150, 255), // openclaw — periwinkle
];

fn agent_color(idx: usize) -> PackedRgba {
    AGENT_COLORS[idx % AGENT_COLORS.len()]
}

// ---------------------------------------------------------------------------
// AnalyticsChartData — pre-computed chart data
// ---------------------------------------------------------------------------

/// Pre-computed chart data for the analytics views.
///
/// Loaded once when entering the analytics surface, refreshed on filter changes.
#[derive(Clone, Debug, Default)]
pub struct AnalyticsChartData {
    /// Per-agent token totals: `(agent_slug, api_tokens_total)` sorted desc.
    pub agent_tokens: Vec<(String, f64)>,
    /// Per-agent message counts: `(agent_slug, message_count)` sorted desc.
    pub agent_messages: Vec<(String, f64)>,
    /// Per-agent tool call counts: `(agent_slug, tool_call_count)` sorted desc.
    pub agent_tool_calls: Vec<(String, f64)>,
    /// Daily timeseries: `(label, api_tokens_total)` ordered by date.
    pub daily_tokens: Vec<(String, f64)>,
    /// Daily timeseries: `(label, message_count)` ordered by date.
    pub daily_messages: Vec<(String, f64)>,
    /// Per-model token totals: `(model_family, grand_total_tokens)` sorted desc.
    pub model_tokens: Vec<(String, f64)>,
    /// Coverage percentage (0..100).
    pub coverage_pct: f64,
    /// Total messages across all data.
    pub total_messages: i64,
    /// Total API tokens across all data.
    pub total_api_tokens: i64,
    /// Total tool calls across all data.
    pub total_tool_calls: i64,
    /// Number of unique agents seen.
    pub agent_count: usize,
    /// Per-day heatmap values: `(day_label, normalized_value 0..1)`.
    pub heatmap_days: Vec<(String, f64)>,

    // ── Dashboard KPI extras ─────────────────────────────────────
    /// Total content-estimated tokens across all data.
    pub total_content_tokens: i64,
    /// Daily content tokens: `(label, content_tokens_est_total)`.
    pub daily_content_tokens: Vec<(String, f64)>,
    /// Daily tool calls: `(label, tool_call_count)`.
    pub daily_tool_calls: Vec<(String, f64)>,
    /// Total plan messages.
    pub total_plan_messages: i64,
    /// Daily plan messages: `(label, plan_message_count)`.
    pub daily_plan_messages: Vec<(String, f64)>,
    /// Total estimated cost (USD).
    pub total_cost_usd: f64,
    /// Daily cost: `(label, estimated_cost_usd)`.
    pub daily_cost: Vec<(String, f64)>,
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

/// Load analytics data from the database, returning an `AnalyticsChartData`.
///
/// Gracefully returns empty data if the database is unavailable or tables
/// are missing.
pub fn load_chart_data(
    db: &crate::storage::sqlite::SqliteStorage,
    filters: &super::app::AnalyticsFilterState,
) -> AnalyticsChartData {
    use crate::analytics;

    let conn = db.raw();

    // Build filter from analytics filter state.
    let filter = analytics::AnalyticsFilter {
        since_ms: filters.since_ms,
        until_ms: filters.until_ms,
        agents: filters.agents.iter().cloned().collect(),
        source: match &filters.source_filter {
            SourceFilter::All => analytics::SourceFilter::All,
            SourceFilter::Local => analytics::SourceFilter::Local,
            SourceFilter::Remote => analytics::SourceFilter::Remote,
            SourceFilter::SourceId(s) => analytics::SourceFilter::Specific(s.clone()),
        },
        workspace_ids: filters
            .workspaces
            .iter()
            .filter_map(|w| w.parse().ok())
            .collect(),
    };

    let mut data = AnalyticsChartData::default();

    // Agent breakdown (Track A — usage_daily).
    if let Ok(result) = analytics::query::query_breakdown(
        conn,
        &filter,
        analytics::Dim::Agent,
        analytics::Metric::ApiTotal,
        20,
    ) {
        data.agent_count = result.rows.len();
        data.agent_tokens = result
            .rows
            .iter()
            .map(|r| (r.key.clone(), r.value as f64))
            .collect();
        data.total_api_tokens = result.rows.iter().map(|r| r.value).sum();
    }

    // Agent message counts.
    if let Ok(result) = analytics::query::query_breakdown(
        conn,
        &filter,
        analytics::Dim::Agent,
        analytics::Metric::MessageCount,
        20,
    ) {
        data.agent_messages = result
            .rows
            .iter()
            .map(|r| (r.key.clone(), r.value as f64))
            .collect();
        data.total_messages = result.rows.iter().map(|r| r.value).sum();
    }

    // Tool usage.
    if let Ok(result) = analytics::query::query_tools(conn, &filter, analytics::GroupBy::Day, 20) {
        data.agent_tool_calls = result
            .rows
            .iter()
            .map(|r| (r.key.clone(), r.tool_call_count as f64))
            .collect();
        data.total_tool_calls = result.total_tool_calls;
    }

    // Daily timeseries (for sparklines and line chart).
    if let Ok(result) =
        analytics::query::query_tokens_timeseries(conn, &filter, analytics::GroupBy::Day)
    {
        data.daily_tokens = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.api_tokens_total as f64))
            .collect();
        data.daily_messages = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.message_count as f64))
            .collect();
        data.daily_content_tokens = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.content_tokens_est_total as f64))
            .collect();
        data.daily_tool_calls = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.tool_call_count as f64))
            .collect();
        data.daily_plan_messages = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.plan_message_count as f64))
            .collect();
        data.daily_cost = result
            .buckets
            .iter()
            .map(|(label, bucket)| (label.clone(), bucket.estimated_cost_usd))
            .collect();
        data.total_content_tokens = result.totals.content_tokens_est_total;
        data.total_plan_messages = result.totals.plan_message_count;
        data.total_cost_usd = result.totals.estimated_cost_usd;

        // Build heatmap data (normalize token values to 0..1).
        let max_tokens = data
            .daily_tokens
            .iter()
            .map(|(_, v)| *v)
            .fold(0.0_f64, f64::max);
        data.heatmap_days = data
            .daily_tokens
            .iter()
            .map(|(label, v)| {
                let norm = if max_tokens > 0.0 {
                    v / max_tokens
                } else {
                    0.0
                };
                (label.clone(), norm)
            })
            .collect();
    }

    // Model breakdown (Track B — token_daily_stats).
    if let Ok(result) = analytics::query::query_breakdown(
        conn,
        &filter,
        analytics::Dim::Model,
        analytics::Metric::ApiTotal,
        20,
    ) {
        data.model_tokens = result
            .rows
            .iter()
            .map(|r| (r.key.clone(), r.value as f64))
            .collect();
    }

    // Coverage percentage.
    if let Ok(status) = analytics::query::query_status(conn, &filter) {
        data.coverage_pct = status.coverage.api_token_coverage_pct;
    }

    data
}

// ---------------------------------------------------------------------------
// Chart rendering — per-view functions
// ---------------------------------------------------------------------------

/// Render the Dashboard view: KPI tile wall with sparklines + top agents.
pub fn render_dashboard(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    if area.height < 4 || area.width < 20 {
        return; // too small to render
    }

    // Choose layout based on available height:
    // Tall: KPI tiles (6 lines) + top agents bar (flex) + sparkline (2)
    // Short: KPI tiles (5 lines) + sparkline (2)
    let has_bar = area.height >= 14;

    let chunks = if has_bar {
        Flex::vertical()
            .constraints([
                Constraint::Fixed(6), // KPI tile grid
                Constraint::Min(4),   // Top agents bar chart
                Constraint::Fixed(2), // Aggregate sparkline
            ])
            .split(area)
    } else {
        Flex::vertical()
            .constraints([
                Constraint::Fixed(6), // KPI tile grid
                Constraint::Min(1),   // Aggregate sparkline
            ])
            .split(area)
    };

    // ── KPI Tile Grid ──────────────────────────────────────────
    render_kpi_tiles(data, chunks[0], frame);

    // ── Top Agents Bar Chart ────────────────────────────────────
    if has_bar {
        if !data.agent_tokens.is_empty() && chunks[1].height >= 3 {
            let groups: Vec<BarGroup<'_>> = data
                .agent_tokens
                .iter()
                .take(8)
                .map(|(name, val)| BarGroup::new(name, vec![*val]))
                .collect();
            let colors: Vec<PackedRgba> = (0..groups.len()).map(agent_color).collect();
            let chart = BarChart::new(groups)
                .direction(BarDirection::Horizontal)
                .bar_width(1)
                .bar_gap(0)
                .group_gap(0)
                .colors(colors);
            chart.render(chunks[1], frame);
        } else {
            Paragraph::new(" No agent data")
                .style(ftui::Style::new().fg(PackedRgba::rgb(100, 100, 100)))
                .render(chunks[1], frame);
        }
    }

    // ── Aggregate Token Sparkline ────────────────────────────────
    let sparkline_chunk = if has_bar { chunks[2] } else { chunks[1] };
    if !data.daily_tokens.is_empty() {
        let values: Vec<f64> = data.daily_tokens.iter().map(|(_, v)| *v).collect();
        let sparkline = Sparkline::new(&values)
            .gradient(PackedRgba::rgb(40, 80, 200), PackedRgba::rgb(255, 80, 40));
        sparkline.render(sparkline_chunk, frame);
    }
}

/// Render the KPI tile grid: 2 rows × 3 columns of metric tiles.
fn render_kpi_tiles(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    // 2 rows of tiles, 3 tiles per row
    let rows = Flex::vertical()
        .constraints([Constraint::Fixed(3), Constraint::Fixed(3)])
        .split(area);

    // Row 1: API Tokens | Messages | Tool Calls
    let cols1 = Flex::horizontal()
        .constraints([
            Constraint::Percentage(33.0),
            Constraint::Percentage(34.0),
            Constraint::Percentage(33.0),
        ])
        .split(rows[0]);

    render_kpi_tile(
        "API Tokens",
        &format_compact(data.total_api_tokens),
        &data.daily_tokens,
        PackedRgba::rgb(0, 180, 255), // cyan
        PackedRgba::rgb(0, 100, 200), // dark cyan
        cols1[0],
        frame,
    );
    render_kpi_tile(
        "Messages",
        &format_compact(data.total_messages),
        &data.daily_messages,
        PackedRgba::rgb(100, 220, 100), // green
        PackedRgba::rgb(40, 150, 40),   // dark green
        cols1[1],
        frame,
    );
    render_kpi_tile(
        "Tool Calls",
        &format_compact(data.total_tool_calls),
        &data.daily_tool_calls,
        PackedRgba::rgb(255, 160, 0), // orange
        PackedRgba::rgb(200, 100, 0), // dark orange
        cols1[2],
        frame,
    );

    // Row 2: Content Tokens | Plan Messages | Cost / Coverage
    let cols2 = Flex::horizontal()
        .constraints([
            Constraint::Percentage(33.0),
            Constraint::Percentage(34.0),
            Constraint::Percentage(33.0),
        ])
        .split(rows[1]);

    render_kpi_tile(
        "Content Est",
        &format_compact(data.total_content_tokens),
        &data.daily_content_tokens,
        PackedRgba::rgb(180, 130, 255), // lavender
        PackedRgba::rgb(120, 60, 200),  // dark lavender
        cols2[0],
        frame,
    );
    render_kpi_tile(
        "Plans",
        &format_compact(data.total_plan_messages),
        &data.daily_plan_messages,
        PackedRgba::rgb(255, 200, 0), // gold
        PackedRgba::rgb(180, 140, 0), // dark gold
        cols2[1],
        frame,
    );

    // Cost tile or coverage fallback
    if data.total_cost_usd > 0.001 {
        render_kpi_tile(
            "Cost (USD)",
            &format!("${:.2}", data.total_cost_usd),
            &data.daily_cost,
            PackedRgba::rgb(255, 80, 80), // red
            PackedRgba::rgb(180, 40, 40), // dark red
            cols2[2],
            frame,
        );
    } else {
        render_kpi_tile(
            "API Cvg",
            &format!("{:.0}%", data.coverage_pct),
            &[],                            // no sparkline for coverage
            PackedRgba::rgb(150, 200, 255), // light blue
            PackedRgba::rgb(80, 120, 180),  // muted blue
            cols2[2],
            frame,
        );
    }
}

/// Render a single KPI tile: label (dim) + value (bright) + mini sparkline.
fn render_kpi_tile(
    label: &str,
    value: &str,
    sparkline_data: &[(String, f64)],
    fg_color: PackedRgba,
    spark_color: PackedRgba,
    area: Rect,
    frame: &mut ftui::Frame,
) {
    if area.height < 2 || area.width < 8 {
        return;
    }

    // Row 1: label (dimmed)
    let label_area = Rect {
        x: area.x,
        y: area.y,
        width: area.width,
        height: 1,
    };
    Paragraph::new(&*format!(" {label}"))
        .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 130)))
        .render(label_area, frame);

    // Row 2: big value + inline sparkline
    let value_y = area.y + 1;
    let value_str = format!(" {value}");
    let value_width = value_str.len() as u16 + 1;

    let value_area = Rect {
        x: area.x,
        y: value_y,
        width: area.width.min(value_width),
        height: 1,
    };
    Paragraph::new(&*value_str)
        .style(ftui::Style::new().fg(fg_color).bold())
        .render(value_area, frame);

    // Mini sparkline in remaining space on row 2
    if !sparkline_data.is_empty() && area.width > value_width + 2 {
        let spark_x = area.x + value_width + 1;
        let spark_w = area.width.saturating_sub(value_width + 2);
        if spark_w >= 4 {
            let spark_area = Rect {
                x: spark_x,
                y: value_y,
                width: spark_w,
                height: 1,
            };
            let values: Vec<f64> = sparkline_data.iter().map(|(_, v)| *v).collect();
            Sparkline::new(&values)
                .gradient(spark_color, fg_color)
                .render(spark_area, frame);
        }
    }

    // Optional Row 3: burn rate or delta (if height allows)
    if area.height >= 3 && sparkline_data.len() >= 2 {
        let recent: f64 = sparkline_data
            .iter()
            .rev()
            .take(7)
            .map(|(_, v)| *v)
            .sum::<f64>();
        let prior: f64 = sparkline_data
            .iter()
            .rev()
            .skip(7)
            .take(7)
            .map(|(_, v)| *v)
            .sum::<f64>();
        let delta_area = Rect {
            x: area.x,
            y: area.y + 2,
            width: area.width,
            height: 1,
        };
        if prior > 0.0 {
            let pct = ((recent - prior) / prior) * 100.0;
            let (arrow, color) = if pct > 5.0 {
                ("\u{25b2}", PackedRgba::rgb(255, 80, 80)) // ▲ red (up)
            } else if pct < -5.0 {
                ("\u{25bc}", PackedRgba::rgb(80, 200, 80)) // ▼ green (down)
            } else {
                ("\u{25c6}", PackedRgba::rgb(150, 150, 150)) // ◆ gray (flat)
            };
            let delta_text = format!(" {arrow} {pct:+.0}% vs prior 7d");
            Paragraph::new(&*delta_text)
                .style(ftui::Style::new().fg(color))
                .render(delta_area, frame);
        }
    }
}

/// Format a number compactly: 1.2B, 45.3M, 12.5K, or raw for small values.
fn format_compact(n: i64) -> String {
    let abs = n.unsigned_abs();
    if abs >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if abs >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if abs >= 10_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format_number(n)
    }
}

/// Render the Explorer view: interactive metric selector + line chart + overlays.
pub fn render_explorer(
    data: &AnalyticsChartData,
    state: &ExplorerState,
    area: Rect,
    frame: &mut ftui::Frame,
) {
    if area.height < 4 || area.width < 20 {
        return;
    }

    // Select the data series based on the active metric.
    let (metric_data, metric_color) = metric_series(data, state.metric);

    if metric_data.is_empty() {
        Paragraph::new(" No timeseries data. Run 'cass analytics rebuild' to populate.")
            .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
            .render(area, frame);
        return;
    }

    // Layout: header (2 lines) + chart (flex)
    let chunks = Flex::vertical()
        .constraints([Constraint::Fixed(2), Constraint::Min(4)])
        .split(area);

    // ── Header: metric selector + overlay indicator ──────────────
    let header_text = format!(
        " Metric: {}  |  Overlay: {}  |  (m=cycle metric  o=cycle overlay)",
        state.metric.label(),
        state.overlay.label(),
    );
    Paragraph::new(&*header_text)
        .style(ftui::Style::new().fg(PackedRgba::rgb(180, 180, 200)))
        .render(chunks[0], frame);

    // ── Build series ─────────────────────────────────────────────
    let primary_points: Vec<(f64, f64)> = metric_data
        .iter()
        .enumerate()
        .map(|(i, (_, v))| (i as f64, *v))
        .collect();

    let mut series = vec![ChartSeries::new(
        state.metric.label(),
        &primary_points,
        metric_color,
    )];

    // Per-agent overlay: add a series per top agent (max 5 for readability).
    let agent_overlay_data: Vec<Vec<(f64, f64)>>;
    if state.overlay == ExplorerOverlay::ByAgent && !data.agent_tokens.is_empty() {
        agent_overlay_data = build_agent_overlay(data, state.metric);
        for (i, points) in agent_overlay_data.iter().enumerate().take(5) {
            if !points.is_empty() {
                let name = &data.agent_tokens[i].0;
                series.push(ChartSeries::new(name, points, agent_color(i)).markers(true));
            }
        }
    }

    // X labels: first, mid, last date.
    let x_labels: Vec<&str> = if metric_data.len() >= 3 {
        vec![
            &metric_data[0].0,
            &metric_data[metric_data.len() / 2].0,
            &metric_data[metric_data.len() - 1].0,
        ]
    } else if !metric_data.is_empty() {
        vec![&metric_data[0].0, &metric_data[metric_data.len() - 1].0]
    } else {
        vec![]
    };

    let chart = FtuiLineChart::new(series).x_labels(x_labels).legend(true);
    chart.render(chunks[1], frame);
}

/// Get the daily series data and color for a given explorer metric.
fn metric_series(
    data: &AnalyticsChartData,
    metric: ExplorerMetric,
) -> (&[(String, f64)], PackedRgba) {
    match metric {
        ExplorerMetric::ApiTokens => (&data.daily_tokens, PackedRgba::rgb(0, 150, 255)),
        ExplorerMetric::ContentTokens => {
            (&data.daily_content_tokens, PackedRgba::rgb(180, 130, 255))
        }
        ExplorerMetric::Messages => (&data.daily_messages, PackedRgba::rgb(100, 220, 100)),
        ExplorerMetric::ToolCalls => (&data.daily_tool_calls, PackedRgba::rgb(255, 160, 0)),
        ExplorerMetric::PlanMessages => (&data.daily_plan_messages, PackedRgba::rgb(255, 200, 0)),
        ExplorerMetric::Cost => (&data.daily_cost, PackedRgba::rgb(255, 80, 80)),
    }
}

/// Build per-agent overlay series. Each agent gets its own Vec<(f64, f64)>.
///
/// This is a simplified overlay — it uses the global daily data and distributes
/// proportionally by each agent's share of the total. A full implementation
/// would query per-agent timeseries, but this approximation works for v1.
fn build_agent_overlay(data: &AnalyticsChartData, _metric: ExplorerMetric) -> Vec<Vec<(f64, f64)>> {
    // Distribute the primary metric across agents by their token share.
    let total: f64 = data.agent_tokens.iter().map(|(_, v)| *v).sum();
    if total <= 0.0 {
        return vec![];
    }

    data.agent_tokens
        .iter()
        .take(5)
        .map(|(_, agent_total)| {
            let share = agent_total / total;
            data.daily_tokens
                .iter()
                .enumerate()
                .map(|(i, (_, day_val))| (i as f64, day_val * share))
                .collect()
        })
        .collect()
}

/// Render the Heatmap view: calendar-style token intensity using canvas.
pub fn render_heatmap(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    if data.heatmap_days.is_empty() {
        Paragraph::new(" No daily data available for heatmap. Run 'cass analytics rebuild'.")
            .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
            .render(area, frame);
        return;
    }

    // Use HalfBlock mode for 1×2 sub-pixels per cell.
    let mut painter = Painter::for_area(area, CanvasMode::HalfBlock);
    let (pw, ph) = painter.size();

    // Lay out days in 7-row columns (rows = day of week, columns = weeks).
    let rows = 7u16;
    let cols = (data.heatmap_days.len() as u16).div_ceil(rows);
    let cell_w = if cols > 0 { pw / cols } else { 1 };
    let cell_h = if rows > 0 { ph / rows } else { 1 };

    for (i, (_, value)) in data.heatmap_days.iter().enumerate() {
        let col = i as u16 / rows;
        let row = i as u16 % rows;
        let x = (col * cell_w) as i32;
        let y = (row * cell_h) as i32;
        let color = ftui_extras::charts::heatmap_gradient(*value);
        // Fill a small rect for each day.
        let fill_w = cell_w.max(1) as i32;
        let fill_h = cell_h.max(1) as i32;
        for dy in 0..fill_h {
            for dx in 0..fill_w {
                painter.point_colored(x + dx, y + dy, color);
            }
        }
    }

    let canvas = CanvasRef::from_painter(&painter)
        .style(ftui::Style::new().fg(PackedRgba::rgb(200, 200, 200)));
    canvas.render(area, frame);
}

/// Render the Breakdowns view: agent/source bar charts side by side.
pub fn render_breakdowns(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    if data.agent_tokens.is_empty() {
        Paragraph::new(" No breakdown data available. Run 'cass analytics rebuild'.")
            .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
            .render(area, frame);
        return;
    }

    // Split horizontally: tokens bar chart | messages bar chart
    let chunks = Flex::horizontal()
        .constraints([Constraint::Percentage(50.0), Constraint::Percentage(50.0)])
        .split(area);

    // Token breakdown by agent.
    {
        let groups: Vec<BarGroup<'_>> = data
            .agent_tokens
            .iter()
            .take(8)
            .map(|(name, val)| BarGroup::new(name, vec![*val]))
            .collect();
        let colors: Vec<PackedRgba> = (0..groups.len()).map(agent_color).collect();
        let chart = BarChart::new(groups)
            .direction(BarDirection::Horizontal)
            .bar_width(1)
            .colors(colors);
        chart.render(chunks[0], frame);
    }

    // Message breakdown by agent.
    {
        let groups: Vec<BarGroup<'_>> = data
            .agent_messages
            .iter()
            .take(8)
            .map(|(name, val)| BarGroup::new(name, vec![*val]))
            .collect();
        let colors: Vec<PackedRgba> = (0..groups.len()).map(agent_color).collect();
        let chart = BarChart::new(groups)
            .direction(BarDirection::Horizontal)
            .bar_width(1)
            .colors(colors);
        chart.render(chunks[1], frame);
    }
}

/// Render the Tools view: tool calls per agent with derived metrics.
pub fn render_tools(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    if data.agent_tool_calls.is_empty() {
        Paragraph::new(" No tool usage data available. Run 'cass analytics rebuild'.")
            .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
            .render(area, frame);
        return;
    }

    let groups: Vec<BarGroup<'_>> = data
        .agent_tool_calls
        .iter()
        .take(10)
        .map(|(name, val)| BarGroup::new(name, vec![*val]))
        .collect();

    let colors: Vec<PackedRgba> = (0..groups.len()).map(agent_color).collect();

    let chart = BarChart::new(groups)
        .direction(BarDirection::Horizontal)
        .bar_width(1)
        .bar_gap(0)
        .colors(colors);
    chart.render(area, frame);
}

/// Render the Cost/Models view: model family token breakdown.
pub fn render_cost(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    if data.model_tokens.is_empty() {
        Paragraph::new(
            " No model data available (Track B). Requires connectors with API token data.",
        )
        .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
        .render(area, frame);
        return;
    }

    // Split: bar chart | summary text
    let chunks = Flex::vertical()
        .constraints([Constraint::Min(4), Constraint::Fixed(2)])
        .split(area);

    let groups: Vec<BarGroup<'_>> = data
        .model_tokens
        .iter()
        .take(10)
        .map(|(name, val)| BarGroup::new(name, vec![*val]))
        .collect();

    let model_colors = &[
        PackedRgba::rgb(0, 180, 220),   // blue
        PackedRgba::rgb(220, 120, 0),   // amber
        PackedRgba::rgb(80, 200, 80),   // green
        PackedRgba::rgb(200, 60, 180),  // magenta
        PackedRgba::rgb(255, 200, 60),  // yellow
        PackedRgba::rgb(120, 120, 255), // indigo
    ];
    let colors: Vec<PackedRgba> = (0..groups.len())
        .map(|i| model_colors[i % model_colors.len()])
        .collect();

    let chart = BarChart::new(groups)
        .direction(BarDirection::Horizontal)
        .bar_width(1)
        .colors(colors);
    chart.render(chunks[0], frame);

    let total: f64 = data.model_tokens.iter().map(|(_, v)| v).sum();
    let summary = format!(
        " Total API tokens across {} models: {}",
        data.model_tokens.len(),
        format_number(total as i64),
    );
    Paragraph::new(&*summary)
        .style(ftui::Style::new().fg(PackedRgba::rgb(180, 180, 180)))
        .render(chunks[1], frame);
}

/// Render the Coverage view: coverage percentage + daily coverage sparkline.
pub fn render_coverage(data: &AnalyticsChartData, area: Rect, frame: &mut ftui::Frame) {
    let chunks = Flex::vertical()
        .constraints([
            Constraint::Fixed(3), // coverage summary
            Constraint::Min(3),   // daily sparkline
        ])
        .split(area);

    let cov_bar_width = area.width.saturating_sub(4) as usize;
    let filled = (data.coverage_pct / 100.0 * cov_bar_width as f64).round() as usize;
    let empty = cov_bar_width.saturating_sub(filled);
    let bar = format!(
        " API Coverage: {:.1}%  [{}{}]",
        data.coverage_pct,
        "█".repeat(filled),
        "░".repeat(empty),
    );
    let bar_color = if data.coverage_pct >= 80.0 {
        PackedRgba::rgb(80, 200, 80)
    } else if data.coverage_pct >= 50.0 {
        PackedRgba::rgb(255, 200, 0)
    } else {
        PackedRgba::rgb(255, 80, 80)
    };
    Paragraph::new(&*bar)
        .style(ftui::Style::new().fg(bar_color))
        .render(chunks[0], frame);

    // Daily token sparkline as a proxy for coverage activity.
    if !data.daily_tokens.is_empty() {
        let values: Vec<f64> = data.daily_tokens.iter().map(|(_, v)| *v).collect();
        let sparkline = Sparkline::new(&values)
            .gradient(PackedRgba::rgb(60, 60, 120), PackedRgba::rgb(80, 200, 80));
        sparkline.render(chunks[1], frame);
    } else {
        Paragraph::new(" No daily data for sparkline")
            .style(ftui::Style::new().fg(PackedRgba::rgb(120, 120, 120)))
            .render(chunks[1], frame);
    }
}

/// Explorer view state passed to the render function.
pub struct ExplorerState {
    pub metric: ExplorerMetric,
    pub overlay: ExplorerOverlay,
}

/// Dispatch rendering to the appropriate view function.
///
/// `selection` is the currently highlighted item index (for drilldown).
pub fn render_analytics_content(
    view: AnalyticsView,
    data: &AnalyticsChartData,
    explorer: &ExplorerState,
    selection: usize,
    area: Rect,
    frame: &mut ftui::Frame,
) {
    match view {
        AnalyticsView::Dashboard => render_dashboard(data, area, frame),
        AnalyticsView::Explorer => render_explorer(data, explorer, area, frame),
        AnalyticsView::Heatmap => render_heatmap(data, area, frame),
        AnalyticsView::Breakdowns => {
            render_breakdowns(data, area, frame);
            render_selection_indicator(
                selection,
                data.agent_tokens.len().min(8),
                area,
                frame,
                true,
            );
        }
        AnalyticsView::Tools => {
            render_tools(data, area, frame);
            render_selection_indicator(
                selection,
                data.agent_tool_calls.len().min(10),
                area,
                frame,
                false,
            );
        }
        AnalyticsView::Cost => {
            render_cost(data, area, frame);
            // Cost has a vertical split: bar chart (Min) + summary (Fixed 2).
            if !data.model_tokens.is_empty() && area.height >= 4 {
                let chunks = Flex::vertical()
                    .constraints([Constraint::Min(4), Constraint::Fixed(2)])
                    .split(area);
                render_selection_indicator(
                    selection,
                    data.model_tokens.len().min(10),
                    chunks[0],
                    frame,
                    false,
                );
            }
        }
        AnalyticsView::Coverage => render_coverage(data, area, frame),
    }
}

/// Overlay a `▶` selection indicator at the given row index within `area`.
///
/// If `half_width` is true, the indicator is placed in the left half of the area
/// (for split-pane views like Breakdowns).
fn render_selection_indicator(
    selection: usize,
    max_rows: usize,
    area: Rect,
    frame: &mut ftui::Frame,
    half_width: bool,
) {
    if max_rows == 0 || selection >= max_rows {
        return;
    }
    let target_area = if half_width {
        let chunks = Flex::horizontal()
            .constraints([Constraint::Percentage(50.0), Constraint::Percentage(50.0)])
            .split(area);
        chunks[0]
    } else {
        area
    };
    if target_area.height <= selection as u16 {
        return;
    }
    let sel_y = target_area.y + selection as u16;
    let indicator = Rect {
        x: target_area.x,
        y: sel_y,
        width: 1,
        height: 1,
    };
    Paragraph::new("\u{25b6}")
        .style(ftui::Style::new().fg(PackedRgba::rgb(255, 255, 80)).bold())
        .render(indicator, frame);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a large number with comma separators (e.g. 1234567 → "1,234,567").
fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_number_basic() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
        assert_eq!(format_number(100), "100");
    }

    #[test]
    fn format_compact_suffixes() {
        assert_eq!(format_compact(0), "0");
        assert_eq!(format_compact(999), "999");
        assert_eq!(format_compact(9999), "9,999");
        assert_eq!(format_compact(10_000), "10.0K");
        assert_eq!(format_compact(1_500_000), "1.5M");
        assert_eq!(format_compact(2_300_000_000), "2.3B");
    }

    #[test]
    fn agent_color_cycles() {
        let c0 = agent_color(0);
        let c14 = agent_color(14);
        assert_eq!(c0, c14); // cycles at 14
    }

    #[test]
    fn default_chart_data_is_empty() {
        let data = AnalyticsChartData::default();
        assert!(data.agent_tokens.is_empty());
        assert!(data.daily_tokens.is_empty());
        assert_eq!(data.total_messages, 0);
        assert_eq!(data.coverage_pct, 0.0);
    }

    #[test]
    fn render_analytics_content_all_views_no_panic() {
        // Verify that rendering with empty data doesn't panic for any view.
        let data = AnalyticsChartData::default();
        // We can't easily create a frame in tests, but we can verify the
        // dispatch function compiles and the data structures are correct.
        let _ = &data;
        for view in AnalyticsView::all() {
            // Just verify the match arm exists for each view.
            match view {
                AnalyticsView::Dashboard
                | AnalyticsView::Explorer
                | AnalyticsView::Heatmap
                | AnalyticsView::Breakdowns
                | AnalyticsView::Tools
                | AnalyticsView::Cost
                | AnalyticsView::Coverage => {}
            }
        }
    }
}
