#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1/api}"
SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
SMTP_PORT="${SMTP_PORT:-25}"
SENDER="${SENDER:-local-check@lpe-ct.test}"
RECIPIENT="${RECIPIENT:-postmaster@example.test}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

smtp_expect() {
  local expected="$1"
  local line
  IFS= read -r -t 10 line <&3 || fail "SMTP server did not answer with $expected"
  line="${line%$'\r'}"
  while [[ "${line:3:1}" == "-" ]]; do
    IFS= read -r -t 10 line <&3 || fail "SMTP multiline response ended unexpectedly"
    line="${line%$'\r'}"
  done
  [[ "$line" == "$expected"* ]] || fail "Unexpected SMTP response, expected $expected: $line"
}

smtp_cmd() {
  printf '%s\r\n' "$1" >&3
  smtp_expect "$2"
}

curl --silent --show-error --fail "${API_URL}/dashboard" >/dev/null \
  || fail "Management API is not reachable through ${API_URL}/dashboard"
pass "Management API reachable"

exec 3<>"/dev/tcp/${SMTP_HOST}/${SMTP_PORT}" || fail "Unable to open SMTP connection to ${SMTP_HOST}:${SMTP_PORT}"
smtp_expect 220
smtp_cmd "EHLO local-test.lpe-ct" 250
smtp_cmd "MAIL FROM:<${SENDER}>" 250
smtp_cmd "RCPT TO:<${RECIPIENT}>" 250
printf 'DATA\r\n' >&3
smtp_expect 354
printf 'Subject: LPE-CT local test\r\n\r\nLocal LPE-CT SMTP test.\r\n.\r\n' >&3
smtp_expect 250
printf 'QUIT\r\n' >&3
smtp_expect 221
exec 3>&-
exec 3<&-
pass "SMTP listener accepted local message"

sleep 1
dashboard="$(curl --silent --show-error --fail "${API_URL}/dashboard")" \
  || fail "Unable to reload dashboard after SMTP test"
[[ "$dashboard" == *"deferred_messages"* ]] || fail "Dashboard response does not include queue metrics"
pass "Queue metrics available after local SMTP test"

echo "LPE-CT local test completed successfully."
