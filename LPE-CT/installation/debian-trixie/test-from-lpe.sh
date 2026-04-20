#!/usr/bin/env bash
set -euo pipefail

CT_HOST="${CT_HOST:?Set CT_HOST to the LPE-CT DMZ host or IP}"
SMTP_PORT="${SMTP_PORT:-25}"
API_URL="${API_URL:-}"
SENDER="${SENDER:?Set SENDER to the real LPE relay sender address}"
RECIPIENT="${RECIPIENT:?Set RECIPIENT to a real mailbox hosted behind LPE-CT}"

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

exec 3<>"/dev/tcp/${CT_HOST}/${SMTP_PORT}" || fail "Unable to open SMTP connection to ${CT_HOST}:${SMTP_PORT}"
smtp_expect 220
smtp_cmd "EHLO lpe-core.local" 250
smtp_cmd "MAIL FROM:<${SENDER}>" 250
smtp_cmd "RCPT TO:<${RECIPIENT}>" 250
printf 'DATA\r\n' >&3
smtp_expect 354
printf 'Subject: LPE to LPE-CT relay path test\r\n\r\nMessage emitted from the LPE LAN side toward LPE-CT.\r\n.\r\n' >&3
smtp_expect 250
printf 'QUIT\r\n' >&3
smtp_expect 221
exec 3>&-
exec 3<&-
pass "LPE-side host can reach the LPE-CT SMTP listener"

if [[ -n "$API_URL" ]]; then
  curl --silent --show-error --fail "${API_URL}/dashboard" >/dev/null \
    || fail "LPE-side host cannot reach management API at ${API_URL}/dashboard"
  pass "LPE-side host can reach the LPE-CT management API"
else
  echo "[SKIP] API_URL not set; management API reachability was not tested."
fi

echo "LPE-to-LPE-CT test completed successfully."
