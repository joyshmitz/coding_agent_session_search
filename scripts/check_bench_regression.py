#!/usr/bin/env python3
"""
check_bench_regression.py

Compares Criterion benchmark results to detect performance regressions.
Used in CI to fail builds that exceed the configured threshold.

Usage:
    python scripts/check_bench_regression.py --threshold 10
    python scripts/check_bench_regression.py --threshold 5 --baseline main --current pr
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check for benchmark regressions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Maximum allowed regression percentage (default: 10)",
    )
    parser.add_argument(
        "--baseline",
        type=str,
        default="main",
        help="Baseline benchmark name (default: main)",
    )
    parser.add_argument(
        "--current",
        type=str,
        default="pr",
        help="Current benchmark name (default: pr)",
    )
    parser.add_argument(
        "--target-dir",
        type=str,
        default="target",
        help="Cargo target directory (default: target)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with error on any regression (regardless of threshold)",
    )
    return parser.parse_args()


def find_criterion_dir(target_dir: str) -> Optional[Path]:
    """Find the criterion benchmark directory."""
    criterion_path = Path(target_dir) / "criterion"
    if criterion_path.exists():
        return criterion_path
    return None


def load_benchmark_estimates(criterion_dir: Path, bench_name: str) -> dict:
    """Load benchmark estimates from criterion JSON files."""
    estimates = {}

    for bench_group in criterion_dir.iterdir():
        if not bench_group.is_dir():
            continue

        for bench in bench_group.iterdir():
            if not bench.is_dir():
                continue

            estimates_file = bench / bench_name / "estimates.json"
            if estimates_file.exists():
                try:
                    with open(estimates_file) as f:
                        data = json.load(f)
                        # Criterion stores estimates with "mean" containing "point_estimate"
                        if "mean" in data and "point_estimate" in data["mean"]:
                            key = f"{bench_group.name}/{bench.name}"
                            estimates[key] = data["mean"]["point_estimate"]
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Warning: Could not parse {estimates_file}: {e}", file=sys.stderr)

    return estimates


def compare_benchmarks(baseline: dict, current: dict, threshold: float) -> tuple[list, list, list]:
    """Compare benchmark results and categorize regressions/improvements."""
    regressions = []
    improvements = []
    unchanged = []

    for name, current_time in current.items():
        if name not in baseline:
            continue

        baseline_time = baseline[name]
        if baseline_time == 0:
            continue

        diff_pct = ((current_time - baseline_time) / baseline_time) * 100

        result = {
            "name": name,
            "baseline_ns": baseline_time,
            "current_ns": current_time,
            "diff_pct": diff_pct,
        }

        if diff_pct > threshold:
            regressions.append(result)
        elif diff_pct < -threshold:
            improvements.append(result)
        else:
            unchanged.append(result)

    return regressions, improvements, unchanged


def format_ns(ns: float) -> str:
    """Format nanoseconds to human-readable string."""
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.2f}s"
    elif ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f}ms"
    elif ns >= 1_000:
        return f"{ns / 1_000:.2f}μs"
    else:
        return f"{ns:.0f}ns"


def print_results(regressions: list, improvements: list, unchanged: list, threshold: float):
    """Print benchmark comparison results."""
    print("\n" + "=" * 60)
    print("BENCHMARK REGRESSION CHECK")
    print("=" * 60 + "\n")

    if regressions:
        print(f"⚠️  REGRESSIONS (>{threshold}% slower):")
        print("-" * 40)
        for r in sorted(regressions, key=lambda x: x["diff_pct"], reverse=True):
            print(f"  {r['name']}")
            print(f"    Baseline: {format_ns(r['baseline_ns'])}")
            print(f"    Current:  {format_ns(r['current_ns'])}")
            print(f"    Change:   +{r['diff_pct']:.1f}%")
        print()

    if improvements:
        print(f"✅ IMPROVEMENTS (>{threshold}% faster):")
        print("-" * 40)
        for i in sorted(improvements, key=lambda x: x["diff_pct"]):
            print(f"  {i['name']}")
            print(f"    Baseline: {format_ns(i['baseline_ns'])}")
            print(f"    Current:  {format_ns(i['current_ns'])}")
            print(f"    Change:   {i['diff_pct']:.1f}%")
        print()

    print(f"📊 SUMMARY:")
    print("-" * 40)
    print(f"  Regressions:  {len(regressions)}")
    print(f"  Improvements: {len(improvements)}")
    print(f"  Unchanged:    {len(unchanged)}")
    print(f"  Threshold:    ±{threshold}%")
    print()


def main():
    args = parse_args()

    criterion_dir = find_criterion_dir(args.target_dir)
    if not criterion_dir:
        print("Warning: No criterion benchmark data found.", file=sys.stderr)
        print("Run benchmarks first: cargo bench --bench <name> -- --save-baseline main", file=sys.stderr)
        # Exit successfully if no benchmark data exists (first run)
        sys.exit(0)

    baseline = load_benchmark_estimates(criterion_dir, args.baseline)
    current = load_benchmark_estimates(criterion_dir, args.current)

    if not baseline:
        print(f"Warning: No baseline '{args.baseline}' benchmark data found.", file=sys.stderr)
        sys.exit(0)

    if not current:
        print(f"Warning: No current '{args.current}' benchmark data found.", file=sys.stderr)
        sys.exit(0)

    regressions, improvements, unchanged = compare_benchmarks(
        baseline, current, args.threshold
    )

    if args.json:
        output = {
            "threshold": args.threshold,
            "regressions": regressions,
            "improvements": improvements,
            "unchanged_count": len(unchanged),
            "has_regressions": len(regressions) > 0,
        }
        print(json.dumps(output, indent=2))
    else:
        print_results(regressions, improvements, unchanged, args.threshold)

    # Exit with error if regressions exceed threshold
    if regressions:
        if args.strict:
            print("❌ FAIL: Regressions detected (--strict mode)", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"❌ FAIL: {len(regressions)} benchmark(s) regressed >{args.threshold}%", file=sys.stderr)
            sys.exit(1)

    print("✅ PASS: No significant regressions detected")
    sys.exit(0)


if __name__ == "__main__":
    main()
