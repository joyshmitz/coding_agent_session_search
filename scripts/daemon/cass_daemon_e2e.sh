#!/usr/bin/env bash
# scripts/daemon/cass_daemon_e2e.sh
# End-to-end daemon fallback flow with structured logs and JSON report.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUN_ID="$(date +"%Y%m%d_%H%M%S")_${RANDOM}"
LOG_ROOT="${PROJECT_ROOT}/target/e2e-daemon"
RUN_DIR="${LOG_ROOT}/run_${RUN_ID}"
LOG_FILE="${RUN_DIR}/run.log"
REPORT_JSON="${RUN_DIR}/report.json"
STDOUT_DIR="${RUN_DIR}/stdout"
STDERR_DIR="${RUN_DIR}/stderr"

SANDBOX_DIR="${RUN_DIR}/sandbox"
BUILD_TARGET_DIR="${RUN_DIR}/target"
DATA_DIR="${SANDBOX_DIR}/cass_data"
CODEX_HOME="${SANDBOX_DIR}/.codex"
HOME_DIR="${SANDBOX_DIR}/home"

NO_BUILD=0
EMBEDDER="hash"
QUERY="binary search"

DAEMON_RETRY_MAX="${CASS_DAEMON_RETRY_MAX:-2}"
DAEMON_BACKOFF_BASE_MS="${CASS_DAEMON_BACKOFF_BASE_MS:-200}"
DAEMON_BACKOFF_MAX_MS="${CASS_DAEMON_BACKOFF_MAX_MS:-5000}"
DAEMON_JITTER_PCT="${CASS_DAEMON_JITTER_PCT:-0.2}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build)
            NO_BUILD=1
            shift
            ;;
        --embedder)
            shift
            if [[ $# -gt 0 ]]; then
                EMBEDDER="$1"
                shift
            fi
            ;;
        --query)
            shift
            if [[ $# -gt 0 ]]; then
                QUERY="$1"
                shift
            fi
            ;;
        --help|-h)
            echo "Usage: $0 [--no-build] [--embedder hash|fastembed] [--query \"text\"]"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

if [[ -t 1 ]]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    CYAN='\033[0;36m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    GREEN='' RED='' CYAN='' YELLOW='' BOLD='' NC=''
fi

mkdir -p "${RUN_DIR}" "${STDOUT_DIR}" "${STDERR_DIR}" "${SANDBOX_DIR}" "${DATA_DIR}" "${CODEX_HOME}" "${HOME_DIR}"

log() {
    local level=$1
    shift
    local msg="$*"
    local ts
    ts=$(date +"%Y-%m-%d %H:%M:%S.%3N" 2>/dev/null || date +"%Y-%m-%d %H:%M:%S")
    echo "[${ts}] [${level}] ${msg}" | tee -a "${LOG_FILE}"
}

json_escape() {
    local s="$1"
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}
    s=${s//$'\r'/\\r}
    s=${s//$'\t'/\\t}
    printf '%s' "$s"
}

now_ms() {
    if date +%s%3N >/dev/null 2>&1; then
        date +%s%3N
        return
    fi
    if command -v python3 >/dev/null 2>&1; then
        python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
        return
    fi
    echo "$(( $(date +%s) * 1000 ))"
}

run_step() {
    local name=$1
    shift
    local stdout_file="${STDOUT_DIR}/${name}.out"
    local stderr_file="${STDERR_DIR}/${name}.err"
    local exit_code

    log "STEP" "${name}: $*"
    set +e
    "$@" >"${stdout_file}" 2>"${stderr_file}"
    exit_code=$?
    set -e

    if [[ $exit_code -eq 0 ]]; then
        log "OK" "${name}"
    else
        log "FAIL" "${name} (exit ${exit_code})"
    fi
    return "$exit_code"
}

log "INFO" "Run directory: ${RUN_DIR}"

if [[ $NO_BUILD -eq 0 ]]; then
    run_step "build" bash -c "cd \"$PROJECT_ROOT\" && CARGO_TARGET_DIR=\"$BUILD_TARGET_DIR\" cargo build"
fi

if [[ -z "${CASS_BIN:-}" ]]; then
    if [[ $NO_BUILD -eq 0 ]]; then
        CASS_BIN="${BUILD_TARGET_DIR}/debug/cass"
    else
        CASS_BIN="${PROJECT_ROOT}/target/debug/cass"
    fi
fi

if [[ ! -x "$CASS_BIN" ]]; then
    log "FAIL" "cass binary not found or not executable at ${CASS_BIN}"
    exit 1
fi

run_step "version" "$CASS_BIN" --version

log "INFO" "Preparing sandbox data"
mkdir -p "${CODEX_HOME}/sessions/2024/11/20"
cat > "${CODEX_HOME}/sessions/2024/11/20/daemon-e2e.jsonl" <<'JSONL'
{"type":"event_msg","timestamp":1732118400000,"payload":{"type":"user_message","message":"Explain daemon fallback behavior"}}
{"type":"response_item","timestamp":1732118401000,"payload":{"role":"assistant","content":"Daemon fallback should be transparent to users."}}
{"type":"event_msg","timestamp":1732118402000,"payload":{"type":"user_message","message":"Add retry logic with jittered backoff"}}
{"type":"response_item","timestamp":1732118403000,"payload":{"role":"assistant","content":"Retries should include randomized jitter to avoid thundering herd."}}
JSONL

export CASS_DATA_DIR="${DATA_DIR}"
export CODEX_HOME="${CODEX_HOME}"
export HOME="${HOME_DIR}"
export CODING_AGENT_SEARCH_NO_UPDATE_PROMPT=1

pushd "${SANDBOX_DIR}" >/dev/null

run_step "index_full" "$CASS_BIN" index --full --data-dir "${DATA_DIR}"
run_step "index_semantic" "$CASS_BIN" index --semantic --embedder "${EMBEDDER}" --data-dir "${DATA_DIR}"

SEARCH_MODEL_FLAGS=()
if [[ "${EMBEDDER}" == "hash" ]]; then
    SEARCH_MODEL_FLAGS=(--model hash)
fi

SEARCH_STDOUT="${STDOUT_DIR}/search.out"
SEARCH_STDERR="${STDERR_DIR}/search.err"
SEARCH_START_MS=$(now_ms)
set +e
"$CASS_BIN" --verbose search "${QUERY}" \
    --mode semantic \
    --daemon \
    --json \
    --data-dir "${DATA_DIR}" \
    "${SEARCH_MODEL_FLAGS[@]}" \
    >"${SEARCH_STDOUT}" 2>"${SEARCH_STDERR}"
SEARCH_EXIT=$?
set -e
SEARCH_END_MS=$(now_ms)
SEARCH_LATENCY_MS=$((SEARCH_END_MS - SEARCH_START_MS))

if [[ $SEARCH_EXIT -eq 0 ]]; then
    log "OK" "search"
else
    log "FAIL" "search (exit ${SEARCH_EXIT})"
fi

ATTEMPT_EMBED=$(grep -c "Attempting daemon embed$" "${SEARCH_STDERR}" || true)
ATTEMPT_RERANK=$(grep -c "Attempting daemon rerank$" "${SEARCH_STDERR}" || true)
FALLBACK_EMBED=$(grep -c "Daemon embed failed; using local embedder" "${SEARCH_STDERR}" || true)
FALLBACK_RERANK=$(grep -c "Daemon rerank failed; using local reranker" "${SEARCH_STDERR}" || true)

count_fallback_reason() {
    local reason=$1
    grep -o "fallback_reason=${reason}" "${SEARCH_STDERR}" | wc -l | tr -d ' '
}

FALLBACK_UNAVAILABLE=$(count_fallback_reason "unavailable")
FALLBACK_TIMEOUT=$(count_fallback_reason "timeout")
FALLBACK_OVERLOADED=$(count_fallback_reason "overloaded")
FALLBACK_ERROR=$(count_fallback_reason "error")
FALLBACK_INVALID=$(count_fallback_reason "invalid")
FALLBACK_BACKOFF=$(count_fallback_reason "backoff")

BACKOFF_VALUES=$(grep -o "backoff_ms=[0-9]*" "${SEARCH_STDERR}" | awk -F= '{print $2}' || true)
if [[ -n "${BACKOFF_VALUES}" ]]; then
    BACKOFF_COUNT=$(echo "${BACKOFF_VALUES}" | wc -l | tr -d ' ')
    BACKOFF_MIN=$(echo "${BACKOFF_VALUES}" | sort -n | head -n 1)
    BACKOFF_MAX=$(echo "${BACKOFF_VALUES}" | sort -n | tail -n 1)
    BACKOFF_AVG=$(echo "${BACKOFF_VALUES}" | awk '{sum+=$1} END { if (NR>0) printf "%.2f", sum/NR; else print "0" }')
else
    BACKOFF_COUNT=0
    BACKOFF_MIN=0
    BACKOFF_MAX=0
    BACKOFF_AVG=0
fi

log "INFO" "Daemon embed attempts: ${ATTEMPT_EMBED}"
log "INFO" "Daemon rerank attempts: ${ATTEMPT_RERANK}"
log "INFO" "Embed fallbacks: ${FALLBACK_EMBED}"
log "INFO" "Rerank fallbacks: ${FALLBACK_RERANK}"
log "INFO" "Fallback reasons - unavailable=${FALLBACK_UNAVAILABLE} timeout=${FALLBACK_TIMEOUT} overloaded=${FALLBACK_OVERLOADED} error=${FALLBACK_ERROR} invalid=${FALLBACK_INVALID} backoff=${FALLBACK_BACKOFF}"
log "INFO" "Backoff samples: ${BACKOFF_COUNT} (min=${BACKOFF_MIN}ms max=${BACKOFF_MAX}ms avg=${BACKOFF_AVG}ms)"
log "INFO" "Search latency: ${SEARCH_LATENCY_MS}ms"

cat > "${REPORT_JSON}" <<EOF
{
  "run_id": "$(json_escape "$RUN_ID")",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "query": "$(json_escape "$QUERY")",
  "embedder": "$(json_escape "$EMBEDDER")",
  "daemon_enabled": true,
  "retry_config": {
    "max_attempts": ${DAEMON_RETRY_MAX},
    "base_delay_ms": ${DAEMON_BACKOFF_BASE_MS},
    "max_delay_ms": ${DAEMON_BACKOFF_MAX_MS},
    "jitter_pct": ${DAEMON_JITTER_PCT}
  },
  "search_exit_code": ${SEARCH_EXIT},
  "latency_ms": ${SEARCH_LATENCY_MS},
  "attempts": {
    "embed": ${ATTEMPT_EMBED},
    "rerank": ${ATTEMPT_RERANK}
  },
  "fallbacks": {
    "embed": ${FALLBACK_EMBED},
    "rerank": ${FALLBACK_RERANK}
  },
  "fallback_reasons": {
    "unavailable": ${FALLBACK_UNAVAILABLE},
    "timeout": ${FALLBACK_TIMEOUT},
    "overloaded": ${FALLBACK_OVERLOADED},
    "error": ${FALLBACK_ERROR},
    "invalid": ${FALLBACK_INVALID},
    "backoff": ${FALLBACK_BACKOFF}
  },
  "backoff_ms": {
    "samples": ${BACKOFF_COUNT},
    "min": ${BACKOFF_MIN},
    "max": ${BACKOFF_MAX},
    "avg": ${BACKOFF_AVG}
  },
  "artifacts": {
    "stdout": "$(json_escape "$SEARCH_STDOUT")",
    "stderr": "$(json_escape "$SEARCH_STDERR")",
    "log": "$(json_escape "$LOG_FILE")"
  }
}
EOF

popd >/dev/null

if [[ $SEARCH_EXIT -ne 0 ]]; then
    log "FAIL" "Daemon E2E run failed. Report: ${REPORT_JSON}"
    exit "$SEARCH_EXIT"
fi

log "OK" "Daemon E2E run completed. Report: ${REPORT_JSON}"
exit 0
