#!/usr/bin/env bash
set -euo pipefail

SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
SMTP_PORT="${SMTP_PORT:-25}"
SENDER="${SENDER:?Set SENDER to a real sender address}"
RECIPIENT="${RECIPIENT:?Set RECIPIENT to a real mailbox hosted behind LPE-CT}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
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

data_reply="$(smtp_expect_code 250)"
printf 'QUIT\r\n' >&3
smtp_expect_code 221 >/dev/null
exec 3>&-
exec 3<&-

[[ "$data_reply" == *"quarantined as"* ]] || fail "Expected antivirus quarantine response, got: $data_reply"
pass "SMTP antivirus scenario quarantined the EICAR attachment"
