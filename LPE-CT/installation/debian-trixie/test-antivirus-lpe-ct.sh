#!/usr/bin/env bash
set -euo pipefail

SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
SMTP_PORT="${SMTP_PORT:-25}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"
SENDER="${SENDER:-}"
RECIPIENT="${RECIPIENT:-}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

usage() {
  cat <<EOF
Usage:
  SENDER=sender@example.net RECIPIENT=user@example.com ./test-antivirus-lpe-ct.sh

Optional overrides:
  SMTP_HOST=${SMTP_HOST}
  SMTP_PORT=${SMTP_PORT}
  SPOOL_DIR=${SPOOL_DIR}

Required environment variables:
  SENDER     Real sender address accepted by the test path
  RECIPIENT  Real mailbox hosted behind LPE-CT
EOF
}

require_env() {
  local name="$1"
  local description="$2"
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    usage >&2
    fail "$name is required. $description"
  fi
}

smtp_expect_line() {
  local line
  IFS= read -r -t 10 line <&3 || fail "SMTP server did not answer"
  line="${line%$'\r'}"
  while [[ "${line:3:1}" == "-" ]]; do
    IFS= read -r -t 10 line <&3 || fail "SMTP multiline response ended unexpectedly"
    line="${line%$'\r'}"
  done
  printf '%s' "$line"
}

smtp_expect_code() {
  local expected="$1"
  local line
  line="$(smtp_expect_line)"
  [[ "$line" == "$expected"* ]] || fail "Unexpected SMTP response, expected $expected: $line"
  printf '%s' "$line"
}

smtp_cmd() {
  local command="$1"
  local expected="$2"
  printf '%s\r\n' "$command" >&3
  smtp_expect_code "$expected" >/dev/null
}

require_env SENDER "Set it to a real sender address before running the antivirus test."
require_env RECIPIENT "Set it to a real mailbox hosted behind LPE-CT before running the antivirus test."

exec 3<>"/dev/tcp/${SMTP_HOST}/${SMTP_PORT}" || fail "Unable to open SMTP connection to ${SMTP_HOST}:${SMTP_PORT}"
smtp_expect_code 220 >/dev/null
smtp_cmd "EHLO antivirus-test.lpe-ct" 250
smtp_cmd "MAIL FROM:<${SENDER}>" 250
smtp_cmd "RCPT TO:<${RECIPIENT}>" 250
printf 'DATA\r\n' >&3
smtp_expect_code 354 >/dev/null

printf 'From: <%s>\r\n' "$SENDER" >&3
printf 'To: <%s>\r\n' "$RECIPIENT" >&3
printf 'Subject: LPE-CT antivirus EICAR scenario\r\n' >&3
printf 'MIME-Version: 1.0\r\n' >&3
printf 'Content-Type: multipart/mixed; boundary="avtest"\r\n' >&3
printf '\r\n' >&3
printf -- '--avtest\r\n' >&3
printf 'Content-Type: text/plain; charset=utf-8\r\n\r\n' >&3
printf 'EICAR antivirus scenario.\r\n' >&3
printf -- '--avtest\r\n' >&3
printf 'Content-Type: application/octet-stream; name="eicar.com"\r\n' >&3
printf 'Content-Disposition: attachment; filename="eicar.com"\r\n' >&3
printf 'Content-Transfer-Encoding: base64\r\n\r\n' >&3
printf 'WDVPIVAlQEFQWzRcUFpYNTQoUF4pN0NDKTd9JEVJQ0FSLVNUQU5EQVJELUFOVElWSVJVUy1URVNU\r\n' >&3
printf 'LUZJTEUhJEgrSCo=\r\n' >&3
printf -- '--avtest--\r\n.\r\n' >&3

data_reply="$(smtp_expect_line)"
printf 'QUIT\r\n' >&3
smtp_expect_code 221 >/dev/null
exec 3>&-
exec 3<&-

trace_id=""
if [[ "$data_reply" == 250*"quarantined as "* ]]; then
  trace_id="${data_reply##*quarantined as }"
elif [[ "$data_reply" == 554*"perimeter policy (trace "*")" ]]; then
  trace_id="${data_reply##*perimeter policy (trace }"
  trace_id="${trace_id%)}"
else
  fail "Expected antivirus quarantine or perimeter reject response, got: $data_reply"
fi

[[ -n "$trace_id" ]] || fail "Unable to extract trace id from SMTP response: $data_reply"

trace_file="${SPOOL_DIR}/quarantine/${trace_id}.json"
for _ in 1 2 3 4 5; do
  [[ -f "$trace_file" ]] && break
  sleep 1
done

[[ -f "$trace_file" ]] || fail "Trace ${trace_id} was not written to ${trace_file}"
grep -qi 'antivirus' "$trace_file" || fail "Trace ${trace_id} does not show an antivirus decision in ${trace_file}"
grep -qi '"status":"\(quarantined\|rejected\)"' "$trace_file" || fail "Trace ${trace_id} is not marked as quarantined or rejected in ${trace_file}"

pass "SMTP antivirus scenario retained the EICAR attachment in quarantine as trace ${trace_id}"
