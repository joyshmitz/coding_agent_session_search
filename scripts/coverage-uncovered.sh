#!/usr/bin/env bash
# coverage-uncovered.sh - Show uncovered code and functions
#
# Useful for identifying untested code paths that need attention.
#
# Usage:
#   ./scripts/coverage-uncovered.sh           # Show uncovered lines
#   ./scripts/coverage-uncovered.sh --fail    # Exit 1 if below threshold

set -euo pipefail

THRESHOLD=60
FAIL_MODE=false

for arg in "$@"; do
    case $arg in
        --fail)
            FAIL_MODE=true
            ;;
        --threshold=*)
            THRESHOLD="${arg#*=}"
            ;;
        --help|-h)
            echo "Usage: $0 [--fail] [--threshold=N]"
            echo ""
            echo "Options:"
            echo "  --fail          Exit with code 1 if coverage below threshold"
            echo "  --threshold=N   Set coverage threshold (default: 60)"
            echo ""
            exit 0
            ;;
    esac
done

# Check if cargo-llvm-cov is installed
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo "Error: cargo-llvm-cov not installed"
    echo ""
    echo "Install with:"
    echo "  rustup component add llvm-tools-preview"
    echo "  cargo install cargo-llvm-cov"
    echo ""
    exit 1
fi

echo "Analyzing uncovered code..."
echo ""

# Show uncovered lines with context
cargo llvm-cov \
    --all-features \
    --workspace \
    --ignore-filename-regex='(tests/|benches/|\.cargo/)' \
    --show-missing-lines \
    -- \
    --skip install_sh \
    --skip install_ps1 \
    2>&1

echo ""
echo "========================================"
echo "  Uncovered Code Analysis Complete"
echo "========================================"
echo ""

# Get coverage percentage
COVERAGE_JSON=$(cargo llvm-cov \
    --all-features \
    --workspace \
    --ignore-filename-regex='(tests/|benches/|\.cargo/)' \
    --json \
    -- \
    --skip install_sh \
    --skip install_ps1 \
    2>/dev/null)

if [ -n "$COVERAGE_JSON" ]; then
    TOTAL_LINES=$(echo "$COVERAGE_JSON" | jq -r '.data[0].totals.lines.count // 0')
    COVERED_LINES=$(echo "$COVERAGE_JSON" | jq -r '.data[0].totals.lines.covered // 0')

    if [ "$TOTAL_LINES" -gt 0 ]; then
        PERCENT=$(echo "scale=2; $COVERED_LINES * 100 / $TOTAL_LINES" | bc)
        UNCOVERED=$((TOTAL_LINES - COVERED_LINES))

        echo "Line coverage: ${PERCENT}%"
        echo "Covered lines: $COVERED_LINES / $TOTAL_LINES"
        echo "Uncovered lines: $UNCOVERED"
        echo ""

        if [ "$FAIL_MODE" = true ]; then
            if (( $(echo "$PERCENT < $THRESHOLD" | bc -l) )); then
                echo "FAIL: Coverage ${PERCENT}% is below ${THRESHOLD}% threshold"
                exit 1
            else
                echo "PASS: Coverage ${PERCENT}% meets ${THRESHOLD}% threshold"
            fi
        fi
    fi
fi
