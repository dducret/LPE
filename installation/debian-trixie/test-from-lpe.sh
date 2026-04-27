#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
API_BASE_URL="${API_BASE_URL:-}"
SENDER="${SENDER:-sender@example.test}"
RECIPIENT="${RECIPIENT:-dest@example.test}"
SUBJECT="${SUBJECT:-LPE to LPE-CT outbound handoff test}"
BODY_TEXT="${BODY_TEXT:-Message emitted from the LPE core side toward the LPE-CT outbound handoff API.}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

warn() {
  echo "[WARN] $*" >&2
}

pass() {
  echo "[OK] $*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr 'A-Z' 'a-z'
    return
  fi
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    tr 'A-Z' 'a-z' < /proc/sys/kernel/random/uuid
    return
  fi
  local hex
  hex="$(openssl rand -hex 16)"
  printf '%s-%s-%s-%s-%s\n' "${hex:0:8}" "${hex:8:4}" "${hex:12:4}" "${hex:16:4}" "${hex:20:12}"
}

safe_json_test_value() {
  local value="$1"
  [[ "${value}" != *\"* && "${value}" != *\\* && "${value}" != *$'\n'* && "${value}" != *$'\r'* ]]
}

host_port_from_url() {
  local url="$1"
  local without_scheme
  local authority
  local host
  local port
  without_scheme="${url#http://}"
  without_scheme="${without_scheme#https://}"
  authority="${without_scheme%%/*}"
  host="${authority%%:*}"
  if [[ "${authority}" == *:* ]]; then
    port="${authority##*:}"
  elif [[ "${url}" == https://* ]]; then
    port="443"
  else
    port="80"
  fi
  printf '%s:%s' "${host}" "${port}"
}

diagnose_tcp_failure() {
  local endpoint="$1"
  local host="${endpoint%:*}"
  local port="${endpoint##*:}"
  echo "[DIAG] ENV_FILE=${ENV_FILE}"
  echo "[DIAG] LPE_CT_API_BASE_URL=${API_BASE_URL}"
  echo "[DIAG] Tested endpoint=${host}:${port}"
  if command -v ss >/dev/null 2>&1; then
    echo "[DIAG] Local listening sockets matching :${port}:"
    ss -ltnp 2>/dev/null | grep -E "[:.]${port}[[:space:]]" || true
  fi
}

require_command curl
require_command openssl
require_command sha256sum

[[ -f "${ENV_FILE}" ]] || fail "Environment file not found: ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

API_BASE_URL="${API_BASE_URL:-${LPE_CT_API_BASE_URL:-}}"
SECRET="${LPE_INTEGRATION_SHARED_SECRET:-}"

[[ -n "${API_BASE_URL}" ]] || fail "LPE_CT_API_BASE_URL is not configured in ${ENV_FILE}"
[[ "${API_BASE_URL}" == http://* || "${API_BASE_URL}" == https://* ]] \
  || fail "LPE_CT_API_BASE_URL must start with http:// or https://, got: ${API_BASE_URL}"
[[ ${#SECRET} -ge 32 ]] || fail "LPE_INTEGRATION_SHARED_SECRET is missing or shorter than 32 characters"

safe_json_test_value "${SENDER}" || fail "SENDER contains characters this shell test cannot JSON-encode safely"
safe_json_test_value "${RECIPIENT}" || fail "RECIPIENT contains characters this shell test cannot JSON-encode safely"
safe_json_test_value "${SUBJECT}" || fail "SUBJECT contains characters this shell test cannot JSON-encode safely"
safe_json_test_value "${BODY_TEXT}" || fail "BODY_TEXT contains characters this shell test cannot JSON-encode safely"

API_BASE_URL="${API_BASE_URL%/}"
ENDPOINT_PATH="/api/v1/integration/outbound-messages"
ENDPOINT_URL="${API_BASE_URL}${ENDPOINT_PATH}"
HEALTH_URL="${API_BASE_URL}/health/live"
TCP_ENDPOINT="$(host_port_from_url "${API_BASE_URL}")"
TCP_HOST="${TCP_ENDPOINT%:*}"
TCP_PORT="${TCP_ENDPOINT##*:}"

timeout 5 bash -c ":</dev/tcp/${TCP_HOST}/${TCP_PORT}" >/dev/null 2>&1 \
  || {
    diagnose_tcp_failure "${TCP_ENDPOINT}"
    fail "LPE-CT API listener is not reachable from LPE on ${TCP_HOST}:${TCP_PORT}"
  }
pass "LPE-CT API listener is reachable from LPE on ${TCP_HOST}:${TCP_PORT}"

health_body="$(curl --silent --show-error --fail "${HEALTH_URL}")" \
  || fail "LPE-CT health endpoint is unreachable: ${HEALTH_URL}"
[[ "${health_body}" == *"\"service\":\"lpe-ct\""* ]] \
  || fail "LPE-CT health endpoint did not return the expected service signature: ${health_body}"
pass "LPE-CT health endpoint is reachable"

queue_id="$(new_uuid)"
message_id="$(new_uuid)"
account_id="$(new_uuid)"
timestamp="$(date +%s)"
nonce="lpe-outbound-test-${timestamp}-${queue_id}"
internet_message_id="<${queue_id}@lpe.test>"

payload="$(printf '{"queue_id":"%s","message_id":"%s","account_id":"%s","from_address":"%s","from_display":null,"sender_address":null,"sender_display":null,"sender_authorization_kind":"self","to":[{"address":"%s","display_name":null}],"cc":[],"bcc":[],"subject":"%s","body_text":"%s","body_html_sanitized":null,"internet_message_id":"%s","attempt_count":0,"last_attempt_error":null}' \
  "${queue_id}" \
  "${message_id}" \
  "${account_id}" \
  "${SENDER}" \
  "${RECIPIENT}" \
  "${SUBJECT}" \
  "${BODY_TEXT}" \
  "${internet_message_id}")"

body_hash="$(printf '%s' "${payload}" | sha256sum | awk '{print $1}')"
signing_input="$(printf 'POST\n%s\n%s\n%s\n%s' "${ENDPOINT_PATH}" "${timestamp}" "${nonce}" "${body_hash}")"
signature="$(printf '%s' "${signing_input}" | openssl dgst -sha256 -hmac "${SECRET}" | awk '{print $NF}')"
response_file="$(mktemp)"
http_status="$(
  curl --silent --show-error \
    --output "${response_file}" \
    --write-out '%{http_code}' \
    -H "content-type: application/json" \
    -H "x-lpe-integration-key: ${SECRET}" \
    -H "x-lpe-integration-timestamp: ${timestamp}" \
    -H "x-lpe-integration-nonce: ${nonce}" \
    -H "x-lpe-integration-signature: ${signature}" \
    -d "${payload}" \
    "${ENDPOINT_URL}"
)"

response_body="$(cat "${response_file}")"
rm -f "${response_file}"

if [[ "${http_status}" != "200" ]]; then
  echo "[DIAG] ${ENDPOINT_URL} returned HTTP ${http_status}:"
  printf '%s\n' "${response_body}"
  fail "Signed LPE -> LPE-CT outbound handoff failed"
fi

[[ "${response_body}" == *"\"queue_id\":\"${queue_id}\""* ]] \
  || fail "LPE-CT response did not echo the test queue_id: ${response_body}"
[[ "${response_body}" == *"\"status\":"* ]] \
  || fail "LPE-CT response did not include a transport status: ${response_body}"

pass "Signed LPE -> LPE-CT outbound handoff endpoint responded"
echo "[INFO] Response: ${response_body}"

if [[ "${SENDER}" == "sender@example.test" || "${RECIPIENT}" == "dest@example.test" ]]; then
  warn "Default example.test addresses were used. For an end-to-end relay test, rerun with real SENDER and RECIPIENT values."
fi

echo "LPE to LPE-CT outbound handoff test completed successfully."
