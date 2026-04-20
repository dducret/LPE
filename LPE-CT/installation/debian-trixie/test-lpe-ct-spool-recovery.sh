#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECOVER_SCRIPT="${RECOVER_SCRIPT:-${SCRIPT_DIR}/lpe-ct-spool-recover.sh}"
TMP_DIR="$(mktemp -d)"
SPOOL_DIR="${TMP_DIR}/spool"

fail() {
  echo "[FAIL] $*" >&2
  rm -rf "${TMP_DIR}"
  exit 1
}

pass() {
  echo "[OK] $*"
}

trap 'rm -rf "${TMP_DIR}"' EXIT

mkdir -p \
  "${SPOOL_DIR}/incoming" \
  "${SPOOL_DIR}/outbound" \
  "${SPOOL_DIR}/deferred" \
  "${SPOOL_DIR}/held" \
  "${SPOOL_DIR}/quarantine" \
  "${SPOOL_DIR}/bounces" \
  "${SPOOL_DIR}/sent"

cat > "${SPOOL_DIR}/deferred/inbound-trace.json" <<'EOF'
{
  "id": "inbound-trace",
  "direction": "inbound",
  "status": "deferred"
}
EOF

cat > "${SPOOL_DIR}/deferred/outbound-trace.json" <<'EOF'
{
  "id": "outbound-trace",
  "direction": "outbound",
  "status": "deferred"
}
EOF

cat > "${SPOOL_DIR}/held/unknown-trace.json" <<'EOF'
{
  "id": "unknown-trace",
  "status": "failed"
}
EOF

summary="$("${RECOVER_SCRIPT}" summary "${SPOOL_DIR}")"
[[ "${summary}" == *"deferred"* ]] || fail "Summary output does not include deferred queue"
pass "Summary reports spool queues"

"${RECOVER_SCRIPT}" requeue deferred all "${SPOOL_DIR}" >/dev/null
[[ -f "${SPOOL_DIR}/incoming/inbound-trace.json" ]] || fail "Inbound deferred trace was not moved back to incoming"
[[ -f "${SPOOL_DIR}/outbound/outbound-trace.json" ]] || fail "Outbound deferred trace was not moved back to outbound"
pass "Deferred queue items are routed back to their owning queues"

"${RECOVER_SCRIPT}" requeue held all "${SPOOL_DIR}" >/dev/null
[[ -f "${SPOOL_DIR}/held/unknown-trace.json" ]] || fail "Unknown held trace should remain in held for manual handling"
pass "Unknown-direction held items stay in held"

trace_output="$("${RECOVER_SCRIPT}" show inbound-trace "${SPOOL_DIR}")"
[[ "${trace_output}" == *'"direction": "inbound"'* ]] || fail "Show command did not return trace contents"
pass "Trace inspection returns JSON payload"

echo "LPE-CT spool recovery scenario completed successfully."
