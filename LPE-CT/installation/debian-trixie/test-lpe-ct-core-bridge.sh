#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
RECIPIENT="${RECIPIENT:-postmaster@example.test}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

[[ -f "$ENV_FILE" ]] || fail "Environment file not found: $ENV_FILE"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v openssl >/dev/null 2>&1 || fail "openssl is required to compute signed integration headers"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum is required"

set -a
source "$ENV_FILE"
set +a

CORE_BASE_URL="${CORE_BASE_URL:-${LPE_CT_CORE_DELIVERY_BASE_URL:-}}"
SECRET="${LPE_INTEGRATION_SHARED_SECRET:-}"
[[ -n "$CORE_BASE_URL" ]] || fail "LPE_CT_CORE_DELIVERY_BASE_URL is not configured"
[[ ${#SECRET} -ge 32 ]] || fail "LPE_INTEGRATION_SHARED_SECRET is missing or shorter than 32 characters"

CORE_BASE_URL="${CORE_BASE_URL%/}"
HEALTH_URL="${CORE_BASE_URL}/health/live"
BRIDGE_PATH="/internal/lpe-ct/recipient-verification"
BRIDGE_URL="${CORE_BASE_URL}${BRIDGE_PATH}"

health_body="$(curl --silent --show-error --fail "$HEALTH_URL")" \
  || fail "LPE health endpoint is unreachable: $HEALTH_URL"
[[ "$health_body" == *"\"service\":\"lpe-admin-api\""* ]] \
  || fail "LPE health endpoint did not return the expected service signature: $health_body"
pass "LPE health endpoint is reachable"

timestamp="$(date +%s)"
nonce="lpe-ct-bridge-test-${timestamp}-$$"
payload="{\"trace_id\":\"lpe-ct-bridge-test-${timestamp}\",\"direction\":\"smtp-inbound\",\"sender\":\"postmaster@lpe-ct.local\",\"recipient\":\"${RECIPIENT}\",\"helo\":\"lpe-ct-bridge-test\",\"peer\":null,\"account_id\":null}"
body_hash="$(printf '%s' "$payload" | sha256sum | awk '{print $1}')"
signing_input="$(printf 'POST\n%s\n%s\n%s\n%s' "$BRIDGE_PATH" "$timestamp" "$nonce" "$body_hash")"
signature="$(printf '%s' "$signing_input" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')"

bridge_body="$(
  curl --silent --show-error --fail \
    -H "content-type: application/json" \
    -H "x-lpe-integration-key: ${SECRET}" \
    -H "x-lpe-integration-timestamp: ${timestamp}" \
    -H "x-lpe-integration-nonce: ${nonce}" \
    -H "x-lpe-integration-signature: ${signature}" \
    -d "$payload" \
    "$BRIDGE_URL"
)" || fail "Signed LPE recipient-verification bridge is unreachable: $BRIDGE_URL"
[[ "$bridge_body" == *"\"verified\":"* ]] \
  || fail "Signed LPE bridge returned an unexpected response: $bridge_body"
pass "Signed LPE recipient-verification bridge responded"

echo "LPE-CT to LPE bridge test completed successfully."
