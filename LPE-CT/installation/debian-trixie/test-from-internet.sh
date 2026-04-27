#!/usr/bin/env bash
set -euo pipefail

CT_PUBLIC_HOST="${CT_PUBLIC_HOST:?Set CT_PUBLIC_HOST to the public MX host or IP}"
SMTP_PORT="${SMTP_PORT:-25}"
EXPECT_MANAGEMENT_PUBLIC="${EXPECT_MANAGEMENT_PUBLIC:-false}"
MANAGEMENT_URL="${MANAGEMENT_URL:-https://${CT_PUBLIC_HOST}/api/dashboard}"
SENDER="${SENDER:-internet-check@example.net}"
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
  IFS= read -r -t 15 line <&3 || fail "SMTP server did not answer with $expected"
  line="${line%$'\r'}"
  while [[ "${line:3:1}" == "-" ]]; do
    IFS= read -r -t 15 line <&3 || fail "SMTP multiline response ended unexpectedly"
    line="${line%$'\r'}"
  done
  [[ "$line" == "$expected"* ]] || fail "Unexpected SMTP response, expected $expected: $line"
}

smtp_cmd() {
  printf '%s\r\n' "$1" >&3
  smtp_expect "$2"
}

exec 3<>"/dev/tcp/${CT_PUBLIC_HOST}/${SMTP_PORT}" || fail "Unable to open SMTP connection to ${CT_PUBLIC_HOST}:${SMTP_PORT}"
smtp_expect 220
smtp_cmd "EHLO internet-test.example.net" 250
smtp_cmd "MAIL FROM:<${SENDER}>" 250
smtp_cmd "RCPT TO:<${RECIPIENT}>" 250
printf 'DATA\r\n' >&3
smtp_expect 354
printf 'Subject: LPE-CT Internet ingress test\r\n\r\nMessage emitted from an Internet-side test machine.\r\n.\r\n' >&3
smtp_expect 250
printf 'QUIT\r\n' >&3
smtp_expect 221
exec 3>&-
exec 3<&-
pass "Internet-side host can reach the public LPE-CT SMTP listener"

if [[ "$EXPECT_MANAGEMENT_PUBLIC" == "true" ]]; then
  curl --silent --show-error --fail --insecure "$MANAGEMENT_URL" >/dev/null \
    || fail "Management URL is expected to be public but is not reachable: $MANAGEMENT_URL"
  pass "Management URL is publicly reachable as expected"
else
  if curl --silent --show-error --fail --insecure --max-time 5 "$MANAGEMENT_URL" >/dev/null 2>&1; then
    fail "Management URL is publicly reachable but EXPECT_MANAGEMENT_PUBLIC is false: $MANAGEMENT_URL"
  fi
  pass "Management URL is not publicly reachable"
fi

echo "Internet-to-LPE-CT test completed successfully."
