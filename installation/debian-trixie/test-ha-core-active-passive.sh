#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
ROLE_FILE="${ROLE_FILE:-}"
ROLE_SCRIPT="${ROLE_SCRIPT:-$(dirname "$0")/lpe-ha-set-role.sh}"
READY_SCRIPT="${READY_SCRIPT:-$(dirname "$0")/check-lpe-ready.sh}"
BIND_ADDRESS="${BIND_ADDRESS:-}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

ROLE_FILE="${ROLE_FILE:-${LPE_HA_ROLE_FILE:-}}"
[[ -n "${ROLE_FILE}" ]] || fail "LPE_HA_ROLE_FILE must be configured before running this test"
BIND_ADDRESS="${BIND_ADDRESS:-${LPE_BIND_ADDRESS:-127.0.0.1:8080}}"

if [[ -f "${ROLE_FILE}" ]]; then
  ORIGINAL_ROLE="$(tr -d '\r\n' < "${ROLE_FILE}")"
else
  ORIGINAL_ROLE=""
fi

restore_role() {
  if [[ -n "${ORIGINAL_ROLE}" ]]; then
    "${ROLE_SCRIPT}" "${ORIGINAL_ROLE}" "${ROLE_FILE}" >/dev/null
  else
    rm -f "${ROLE_FILE}"
  fi
}

trap restore_role EXIT

assert_ready() {
  local expected="$1"
  local body
  body="$(curl --silent --show-error --fail "http://${BIND_ADDRESS}/health/ready")" \
    || fail "Unable to query http://${BIND_ADDRESS}/health/ready"
  [[ "${body}" == *"\"status\":\"${expected}\""* ]] \
    || fail "Expected readiness status ${expected}, got: ${body}"
}

"${ROLE_SCRIPT}" active "${ROLE_FILE}" >/dev/null
assert_ready ready
"${READY_SCRIPT}" || fail "Readiness probe should succeed in active role"
pass "Active role returns ready and passes the readiness probe"

"${ROLE_SCRIPT}" standby "${ROLE_FILE}" >/dev/null
assert_ready failed
if "${READY_SCRIPT}" >/dev/null 2>&1; then
  fail "Readiness probe should fail in standby role"
fi
pass "Standby role is not traffic-ready"

"${ROLE_SCRIPT}" drain "${ROLE_FILE}" >/dev/null
assert_ready failed
if "${READY_SCRIPT}" >/dev/null 2>&1; then
  fail "Readiness probe should fail in drain role"
fi
pass "Drain role is not traffic-ready"

"${ROLE_SCRIPT}" maintenance "${ROLE_FILE}" >/dev/null
assert_ready failed
if "${READY_SCRIPT}" >/dev/null 2>&1; then
  fail "Readiness probe should fail in maintenance role"
fi
pass "Maintenance role is not traffic-ready"

"${ROLE_SCRIPT}" active "${ROLE_FILE}" >/dev/null
assert_ready ready
pass "Role switch back to active succeeds"

echo "LPE core HA role validation completed successfully."
