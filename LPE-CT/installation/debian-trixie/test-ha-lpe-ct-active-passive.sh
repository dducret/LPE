#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
ROLE_FILE="${ROLE_FILE:-}"
ROLE_SCRIPT="${ROLE_SCRIPT:-$(dirname "$0")/lpe-ct-ha-set-role.sh}"
READY_SCRIPT="${READY_SCRIPT:-$(dirname "$0")/check-lpe-ct-ready.sh}"
BIND_ADDRESS="${BIND_ADDRESS:-}"
SMTP_HOST="${SMTP_HOST:-}"
SMTP_PORT="${SMTP_PORT:-}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

new_uuid() {
  cat /proc/sys/kernel/random/uuid
}

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

ROLE_FILE="${ROLE_FILE:-${LPE_CT_HA_ROLE_FILE:-}}"
[[ -n "${ROLE_FILE}" ]] || fail "LPE_CT_HA_ROLE_FILE must be configured before running this test"
[[ -n "${LPE_INTEGRATION_SHARED_SECRET:-}" ]] || fail "LPE_INTEGRATION_SHARED_SECRET must be configured before running this test"
BIND_ADDRESS="${BIND_ADDRESS:-${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}}"
SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
SMTP_PORT="${SMTP_PORT:-${LPE_CT_SMTP_BIND_ADDRESS##*:}}"

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

smtp_first_line() {
  timeout 10 bash -lc "exec 3<>/dev/tcp/${SMTP_HOST}/${SMTP_PORT}; IFS= read -r line <&3; printf '%s\n' \"\$line\"; exec 3<&-; exec 3>&-"
}

assert_outbound_handoff_status() {
  local expected_status="$1"
  local queue_id
  local message_id
  local account_id
  local status
  queue_id="$(new_uuid)"
  message_id="$(new_uuid)"
  account_id="$(new_uuid)"
  status="$(
    curl --silent --output /tmp/lpe-ct-ha-response.$$ --write-out '%{http_code}' \
      -H "x-lpe-integration-key: ${LPE_INTEGRATION_SHARED_SECRET}" \
      -H 'content-type: application/json' \
      -d "{\"queue_id\":\"${queue_id}\",\"message_id\":\"${message_id}\",\"account_id\":\"${account_id}\",\"from_address\":\"sender@example.test\",\"from_display\":null,\"to\":[{\"address\":\"dest@example.test\",\"display_name\":null}],\"cc\":[],\"bcc\":[],\"subject\":\"HA gating\",\"body_text\":\"test\",\"body_html_sanitized\":null,\"internet_message_id\":null,\"attempt_count\":0,\"last_attempt_error\":null}" \
      "http://${BIND_ADDRESS}/api/v1/integration/outbound-messages"
  )"
  rm -f /tmp/lpe-ct-ha-response.$$
  [[ "${status}" == "${expected_status}" ]] \
    || fail "Expected outbound handoff HTTP ${expected_status}, got ${status}"
}

"${ROLE_SCRIPT}" active "${ROLE_FILE}" >/dev/null
assert_ready ready
"${READY_SCRIPT}" || fail "Readiness probe should succeed in active role"
[[ "$(smtp_first_line)" == 220* ]] || fail "SMTP should advertise a 220 banner in active role"
pass "Active role returns ready and accepts SMTP traffic"

"${ROLE_SCRIPT}" standby "${ROLE_FILE}" >/dev/null
assert_ready failed
if "${READY_SCRIPT}" >/dev/null 2>&1; then
  fail "Readiness probe should fail in standby role"
fi
[[ "$(smtp_first_line)" == 421* ]] || fail "SMTP should return 421 in standby role"
assert_outbound_handoff_status 503
pass "Standby role refuses SMTP and outbound handoff traffic"

"${ROLE_SCRIPT}" drain "${ROLE_FILE}" >/dev/null
assert_ready failed
if "${READY_SCRIPT}" >/dev/null 2>&1; then
  fail "Readiness probe should fail in drain role"
fi
[[ "$(smtp_first_line)" == 421* ]] || fail "SMTP should return 421 in drain role"
assert_outbound_handoff_status 503
pass "Drain role refuses new traffic while staying manageable"

"${ROLE_SCRIPT}" active "${ROLE_FILE}" >/dev/null
assert_ready ready
pass "Role switch back to active succeeds"

echo "LPE-CT active/passive HA validation completed successfully."
