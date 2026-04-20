#!/usr/bin/env bash
set -euo pipefail

COMMAND="${1:-summary}"
QUEUE="${2:-}"
TRACE_SELECTOR="${3:-all}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
Usage:
  lpe-ct-spool-recover.sh summary [spool-dir]
  lpe-ct-spool-recover.sh show <trace-id> [spool-dir]
  lpe-ct-spool-recover.sh requeue <deferred|held> [trace-id|all] [spool-dir]

Notes:
  - deferred/held items are routed back to incoming or outbound according to the JSON "direction".
  - quarantine is intentionally excluded from bulk requeue; release it only after manual review.
EOF
  exit 1
}

count_queue() {
  local queue="$1"
  find "${SPOOL_DIR}/${queue}" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' '
}

target_queue_for_file() {
  local file="$1"
  local direction
  direction="$(grep -m1 '"direction"' "${file}" | sed -E 's/.*"direction"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')" || true
  case "${direction}" in
    inbound)
      echo "incoming"
      ;;
    outbound)
      echo "outbound"
      ;;
    *)
      echo "held"
      ;;
  esac
}

show_summary() {
  echo "Spool root: ${SPOOL_DIR}"
  for queue in incoming outbound deferred held quarantine bounces sent; do
    printf '%-12s %s\n' "${queue}" "$(count_queue "${queue}")"
  done
}

show_trace() {
  local trace_id="$1"
  local file
  for queue in incoming outbound deferred held quarantine bounces sent; do
    file="${SPOOL_DIR}/${queue}/${trace_id}.json"
    if [[ -f "${file}" ]]; then
      echo "Queue: ${queue}"
      cat "${file}"
      return 0
    fi
  done
  fail "Trace not found: ${trace_id}"
}

requeue_one() {
  local source_queue="$1"
  local file="$2"
  local trace_id
  local target_queue

  trace_id="$(basename "${file}" .json)"
  target_queue="$(target_queue_for_file "${file}")"
  [[ -d "${SPOOL_DIR}/${target_queue}" ]] || fail "Missing target queue directory: ${SPOOL_DIR}/${target_queue}"

  if [[ "${source_queue}" == "${target_queue}" ]]; then
    echo "[OK] ${trace_id}: ${source_queue} -> ${target_queue} (unchanged)"
    return 0
  fi

  mv "${file}" "${SPOOL_DIR}/${target_queue}/${trace_id}.json"
  echo "[OK] ${trace_id}: ${source_queue} -> ${target_queue}"
}

requeue_queue() {
  local source_queue="$1"
  local selector="$2"
  local matched=0
  local file

  [[ "${source_queue}" == "deferred" || "${source_queue}" == "held" ]] \
    || fail "Only deferred and held queues support automated requeue"

  shopt -s nullglob
  for file in "${SPOOL_DIR}/${source_queue}"/*.json; do
    local trace_id
    trace_id="$(basename "${file}" .json)"
    if [[ "${selector}" != "all" && "${selector}" != "${trace_id}" ]]; then
      continue
    fi
    requeue_one "${source_queue}" "${file}"
    matched=1
  done
  shopt -u nullglob

  [[ "${matched}" -eq 1 ]] || fail "No matching JSON trace found in ${source_queue} for selector ${selector}"
}

case "${COMMAND}" in
  summary)
    if [[ -n "${QUEUE}" ]]; then
      SPOOL_DIR="${QUEUE}"
    fi
    show_summary
    ;;
  show)
    [[ -n "${QUEUE}" ]] || usage
    if [[ "${TRACE_SELECTOR}" != "all" ]]; then
      SPOOL_DIR="${TRACE_SELECTOR}"
    fi
    show_trace "${QUEUE}"
    ;;
  requeue)
    [[ -n "${QUEUE}" ]] || usage
    if [[ $# -ge 4 ]]; then
      SPOOL_DIR="$4"
    fi
    requeue_queue "${QUEUE}" "${TRACE_SELECTOR}"
    ;;
  *)
    usage
    ;;
esac
