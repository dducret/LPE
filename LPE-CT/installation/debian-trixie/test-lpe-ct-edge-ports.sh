#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
HOST="${HOST:-127.0.0.1}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"
TEST_SCOPE="${LPE_CT_EDGE_TEST_SCOPE:-all}"

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

scope_enabled() {
  local scope="$1"
  [[ "${TEST_SCOPE}" == "all" \
    || ",${TEST_SCOPE}," == *",${scope},"* \
    || ( ",${TEST_SCOPE}," == *",outlook,"* && "${scope}" =~ ^(https|submission|imaps)$ ) ]]
}

explicit_scope_enabled() {
  local scope="$1"
  [[ "${TEST_SCOPE}" == "all" || ",${TEST_SCOPE}," == *",${scope},"* ]]
}

outlook_scope_enabled() {
  [[ ",${TEST_SCOPE}," == *",outlook,"* ]]
}

validate_test_scope() {
  local -a tokens
  local token
  IFS=',' read -ra tokens <<<"${TEST_SCOPE}"
  for token in "${tokens[@]}"; do
    case "${token}" in
      all|smtp|https|submission|imaps|outlook)
        ;;
      *)
        fail "Unsupported LPE_CT_EDGE_TEST_SCOPE token '${token}'. Use all, smtp, https, submission, imaps, outlook, or a comma-separated subset."
        ;;
    esac
  done
}

tls_server_name() {
  printf '%s' "${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_PUBLICATION_TEST_HOST:-${HOST:-${LPE_CT_SERVER_NAME:-localhost}}}}"
}

imap_quote() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

smtp_auth_token() {
  printf '%s' "$1" | base64 | tr -d '\r\n'
}

[[ -f "$ENV_FILE" ]] || fail "Environment file not found: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a
validate_test_scope

listener_snapshot() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    echo "[DIAG] Listening sockets matching :${port}:"
    ss -ltnp 2>/dev/null | grep -E "[:.]${port}[[:space:]]" || true
  else
    echo "[DIAG] ss is not available; cannot list listening sockets."
  fi
}

service_snapshot() {
  if command -v systemctl >/dev/null 2>&1; then
    echo "[DIAG] ${SERVICE_NAME} active state:"
    systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true
    echo "[DIAG] ${SERVICE_NAME} status summary:"
    systemctl --no-pager --lines=8 status "${SERVICE_NAME}" 2>/dev/null || true
  fi
}

recent_logs() {
  if command -v journalctl >/dev/null 2>&1; then
    echo "[DIAG] Recent ${SERVICE_NAME} logs:"
    journalctl -u "${SERVICE_NAME}" --no-pager -n 30 2>/dev/null || true
  fi
}

service_user_can_read() {
  local path="$1"
  if [[ -z "${path}" ]]; then
    return 1
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo -u "${SERVICE_USER}" test -r "${path}" >/dev/null 2>&1
  elif command -v runuser >/dev/null 2>&1; then
    runuser -u "${SERVICE_USER}" -- test -r "${path}" >/dev/null 2>&1
  else
    test -r "${path}"
  fi
}

assert_https_security_headers() {
  local headers_file="$1"
  local label="$2"
  local required_headers=(
    '^strict-transport-security: max-age=31536000'
    '^x-content-type-options: nosniff'
    '^referrer-policy: no-referrer'
    '^x-frame-options: DENY'
    "^content-security-policy: frame-ancestors 'none'"
    '^permissions-policy:'
  )
  local header

  for header in "${required_headers[@]}"; do
    grep -qi "${header}" "${headers_file}" \
      || {
        sed -n '1,40p' "${headers_file}" || true
        fail "${label} is missing expected HTTPS security header matching: ${header}"
      }
  done
}

check_tls_file_readable() {
  local label="$1"
  local path="$2"
  [[ -n "${path}" ]] || return 0
  if service_user_can_read "${path}"; then
    pass "${label} is readable by ${SERVICE_USER}: ${path}"
    return 0
  fi
  echo "[DIAG] ${label} is not readable by ${SERVICE_USER}: ${path}"
  if command -v namei >/dev/null 2>&1; then
    echo "[DIAG] Path permissions for ${path}:"
    namei -l "${path}" || true
  fi
  fail "${label} must be readable by ${SERVICE_USER}; fix owner/group/mode under /etc/lpe-ct/tls before testing edge ports."
}

diagnose_tcp_failure() {
  local name="$1"
  local host="$2"
  local port="$3"
  echo "[DIAG] ${name} TCP probe failed on ${host}:${port}."
  echo "[DIAG] ENV_FILE=${ENV_FILE}"
  echo "[DIAG] LPE_CT_SMTP_BIND_ADDRESS=${LPE_CT_SMTP_BIND_ADDRESS:-unset}"
  echo "[DIAG] LPE_CT_SMTP_HOST=${LPE_CT_SMTP_HOST:-unset}"
  echo "[DIAG] LPE_CT_SMTP_PORT=${LPE_CT_SMTP_PORT:-unset}"
  listener_snapshot "${port}"
  service_snapshot
  recent_logs
  if [[ "${port}" =~ ^[0-9]+$ && "${port}" -lt 1024 ]]; then
    echo "[DIAG] Port ${port} is privileged. The systemd unit must grant CAP_NET_BIND_SERVICE to ${SERVICE_NAME}."
  fi
  warn "If LPE_CT_SMTP_BIND_ADDRESS is a specific LAN IP, rerun this test with HOST=<that-ip>."
}

tcp_probe() {
  local name="$1"
  local host="$2"
  local port="$3"
  timeout 5 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 \
    || {
      diagnose_tcp_failure "${name}" "${host}" "${port}"
      fail "${name} port ${port} is not reachable on ${host}"
    }
  pass "${name} port ${port} is reachable on ${host}"
}

tls_probe_if_possible() {
  local host="$1"
  local port="$2"
  local name="$3"
  local output_file
  local openssl_status=0
  local server_name
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; TCP reachability for ${name} was checked, TLS handshake was not."
    return
  fi
  server_name="$(tls_server_name)"
  output_file="$(mktemp)"
  timeout 10 openssl s_client -showcerts -connect "${host}:${port}" -servername "${server_name}" </dev/null >"${output_file}" 2>&1 || openssl_status=$?
  if grep -q "BEGIN CERTIFICATE" "${output_file}"; then
    if [[ "${openssl_status}" -ne 0 ]]; then
      echo "[WARN] ${name} on ${host}:${port} presented a TLS certificate, but openssl exited with status ${openssl_status}. A proxied service may have closed the session after TLS."
      sed -n '1,40p' "${output_file}" || true
    fi
    rm -f "${output_file}"
    pass "${name} on ${host}:${port} presented a TLS certificate"
    return
  fi
  if [[ "${openssl_status}" -ne 0 ]]; then
    echo "[DIAG] openssl s_client failed for ${name} on ${host}:${port}:"
    sed -n '1,80p' "${output_file}" || true
    recent_logs
    rm -f "${output_file}"
    fail "${name} on ${host}:${port} failed TLS handshake"
  fi
  echo "[DIAG] openssl s_client did not report a certificate for ${name} on ${host}:${port}:"
  sed -n '1,120p' "${output_file}" || true
  recent_logs
  rm -f "${output_file}"
  fail "${name} on ${host}:${port} did not present a TLS certificate"
}

tls_trust_probe_if_possible() {
  local host="$1"
  local port="$2"
  local name="$3"
  local output_file
  local verify_mode="${LPE_CT_EDGE_TLS_VERIFY:-warn}"
  local server_name
  local openssl_status=0

  [[ "${verify_mode}" != "skip" ]] || return 0
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; trusted TLS verification for ${name} was not checked."
    return 0
  fi

  server_name="$(tls_server_name)"
  output_file="$(mktemp)"
  timeout 10 openssl s_client -verify_return_error -verify_hostname "${server_name}" \
    -connect "${host}:${port}" \
    -servername "${server_name}" </dev/null >"${output_file}" 2>&1 || openssl_status=$?

  if grep -q "Verify return code: 0 (ok)" "${output_file}"; then
    if [[ "${openssl_status}" -ne 0 ]]; then
      warn "${name} on ${host}:${port} passed trusted TLS verification, but openssl exited with status ${openssl_status} after the session closed."
    fi
    rm -f "${output_file}"
    pass "${name} on ${host}:${port} has a trusted TLS certificate for ${server_name}"
    return 0
  fi

  echo "[DIAG] ${name} TLS trust/hostname verification failed for ${server_name} on ${host}:${port}:"
  sed -n '1,80p' "${output_file}" || true
  rm -f "${output_file}"
  if [[ "${verify_mode}" == "required" ]] || outlook_scope_enabled; then
    fail "${name} on ${host}:${port} must present a trusted certificate matching ${server_name} for Outlook"
  fi
  warn "${name} on ${host}:${port} did not pass trusted TLS verification for ${server_name}; set LPE_CT_EDGE_TLS_VERIFY=required to fail on this."
}

smtp_starttls_probe_if_possible() {
  local host="$1"
  local port="$2"
  local output_file
  local openssl_status=0
  local server_name
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; SMTP STARTTLS handshake was not checked."
    return
  fi
  server_name="$(tls_server_name)"
  output_file="$(mktemp)"
  timeout 10 openssl s_client -starttls smtp -crlf -showcerts -connect "${host}:${port}" -servername "${server_name}" </dev/null >"${output_file}" 2>&1 || openssl_status=$?
  if grep -q "BEGIN CERTIFICATE" "${output_file}"; then
    if [[ "${openssl_status}" -ne 0 ]]; then
      echo "[WARN] SMTP STARTTLS on ${host}:${port} presented a TLS certificate, but openssl exited with status ${openssl_status}. The service may have closed the SMTP session after TLS."
      sed -n '1,40p' "${output_file}" || true
    fi
    rm -f "${output_file}"
    pass "SMTP ingress on ${host}:${port} completed STARTTLS and presented a TLS certificate"
    return
  fi
  echo "[DIAG] openssl s_client -starttls smtp failed for SMTP ingress on ${host}:${port}:"
  sed -n '1,100p' "${output_file}" || true
  recent_logs
  rm -f "${output_file}"
  fail "SMTP ingress on ${host}:${port} failed STARTTLS handshake"
}

probe_submission_auth_if_configured() {
  local test_email="${LPE_CT_SUBMISSION_TEST_EMAIL:-${LPE_CT_IMAPS_TEST_EMAIL:-${LPE_IMAP_TEST_EMAIL:-${IMAP_TEST_EMAIL:-}}}}"
  local test_password="${LPE_CT_SUBMISSION_TEST_PASSWORD:-${LPE_CT_IMAPS_TEST_PASSWORD:-${LPE_IMAP_TEST_PASSWORD:-${IMAP_TEST_PASSWORD:-}}}}"
  local test_recipient="${LPE_CT_SUBMISSION_TEST_RCPT:-${test_email}}"
  local output_file
  local openssl_status=0
  local username_token
  local password_token
  local server_name

  if [[ -z "${test_email}" || -z "${test_password}" ]]; then
    if outlook_scope_enabled; then
      fail "Outlook scope requires LPE_CT_SUBMISSION_TEST_EMAIL and LPE_CT_SUBMISSION_TEST_PASSWORD, or the IMAPS test credential variables, so SMTP submission AUTH on 465 is verified."
    fi
    warn "Skipping authenticated SMTPS submission probe; set LPE_CT_SUBMISSION_TEST_EMAIL and LPE_CT_SUBMISSION_TEST_PASSWORD to verify Outlook-facing 465 login."
    return 0
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    warn "Skipping authenticated SMTPS submission probe; openssl is not available."
    return 0
  fi
  if ! command -v base64 >/dev/null 2>&1; then
    warn "Skipping authenticated SMTPS submission probe; base64 is not available."
    return 0
  fi

  username_token="$(smtp_auth_token "${test_email}")"
  password_token="$(smtp_auth_token "${test_password}")"
  server_name="$(tls_server_name)"
  output_file="$(mktemp)"

  timeout 20 openssl s_client -quiet -crlf \
    -connect "${HOST}:${SUBMISSION_PORT}" \
    -servername "${server_name}" >"${output_file}" 2>&1 <<EOF || openssl_status=$?
EHLO outlook.lpe.test
AUTH LOGIN
${username_token}
${password_token}
MAIL FROM:<${test_email}>
RCPT TO:<${test_recipient}>
RSET
QUIT
EOF

  if ! grep -q '^220 LPE-CT ESMTP submission ready' "${output_file}"; then
    echo "[DIAG] SMTPS submission probe did not receive the submission greeting."
    recent_logs
    rm -f "${output_file}"
    fail "SMTPS submission on ${HOST}:${SUBMISSION_PORT} did not return the expected greeting"
  fi
  if ! grep -q '^250-AUTH PLAIN LOGIN' "${output_file}"; then
    echo "[DIAG] SMTPS submission EHLO did not advertise AUTH PLAIN LOGIN."
    sed -n '1,80p' "${output_file}" || true
    recent_logs
    rm -f "${output_file}"
    fail "SMTPS submission on ${HOST}:${SUBMISSION_PORT} did not advertise Outlook-compatible AUTH"
  fi
  if ! grep -q '^235 authentication succeeded' "${output_file}"; then
    echo "[DIAG] SMTPS submission AUTH LOGIN failed for the supplied test account."
    sed -n '1,120p' "${output_file}" || true
    recent_logs
    rm -f "${output_file}"
    fail "SMTPS submission on ${HOST}:${SUBMISSION_PORT} failed authenticated AUTH LOGIN"
  fi
  if ! grep -q '^250 sender accepted' "${output_file}"; then
    echo "[DIAG] SMTPS submission authenticated but MAIL FROM was not accepted."
    sed -n '1,120p' "${output_file}" || true
    recent_logs
    rm -f "${output_file}"
    fail "SMTPS submission on ${HOST}:${SUBMISSION_PORT} rejected MAIL FROM for ${test_email}"
  fi
  if ! grep -q '^250 recipient accepted' "${output_file}"; then
    echo "[DIAG] SMTPS submission authenticated but RCPT TO was not accepted."
    sed -n '1,120p' "${output_file}" || true
    recent_logs
    rm -f "${output_file}"
    fail "SMTPS submission on ${HOST}:${SUBMISSION_PORT} rejected RCPT TO for ${test_recipient}"
  fi

  if [[ "${openssl_status}" -ne 0 ]]; then
    warn "Authenticated SMTPS submission probe succeeded, but openssl exited with status ${openssl_status} after the SMTP session closed."
  fi
  rm -f "${output_file}"
  pass "SMTPS submission on ${HOST}:${SUBMISSION_PORT} accepted AUTH LOGIN, MAIL FROM, and RCPT TO through LPE-CT"
}

probe_imaps_upstream() {
  local upstream="$1"
  local host
  local port
  [[ -n "${upstream}" ]] || fail "LPE_CT_IMAPS_UPSTREAM_ADDRESS is required when IMAPS is enabled."
  host="${upstream%:*}"
  port="${upstream##*:}"
  if [[ -z "${host}" || -z "${port}" || "${host}" == "${upstream}" ]]; then
    fail "LPE_CT_IMAPS_UPSTREAM_ADDRESS must be a host:port address, got: ${upstream}"
  fi
  timeout 5 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 \
    || {
      echo "[DIAG] LPE_CT_IMAPS_UPSTREAM_ADDRESS=${upstream}"
      echo "[DIAG] LPE-CT accepted TLS on ${HOST}:${IMAPS_PORT}, but could not reach the core LPE IMAP upstream."
      fail "LPE IMAP upstream ${upstream} is not reachable from LPE-CT; set LPE_CT_IMAPS_UPSTREAM_ADDRESS to the core LPE IMAP listener and ensure that listener is running."
    }
  pass "LPE IMAP upstream ${upstream} is reachable from LPE-CT"
}

probe_public_imaps_auth_if_configured() {
  local test_email="${LPE_CT_IMAPS_TEST_EMAIL:-${LPE_IMAP_TEST_EMAIL:-${IMAP_TEST_EMAIL:-}}}"
  local test_password="${LPE_CT_IMAPS_TEST_PASSWORD:-${LPE_IMAP_TEST_PASSWORD:-${IMAP_TEST_PASSWORD:-}}}"
  local output_file
  local openssl_status=0
  local quoted_email
  local quoted_password
  local server_name

  if [[ -z "${test_email}" || -z "${test_password}" ]]; then
    if outlook_scope_enabled; then
      fail "Outlook scope requires LPE_CT_IMAPS_TEST_EMAIL and LPE_CT_IMAPS_TEST_PASSWORD so the public IMAPS login path is actually verified."
    fi
    warn "Skipping authenticated public IMAPS probe; set LPE_CT_IMAPS_TEST_EMAIL and LPE_CT_IMAPS_TEST_PASSWORD to verify Outlook-facing 993 login."
    return 0
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    warn "Skipping authenticated public IMAPS probe; openssl is not available."
    return 0
  fi

  quoted_email="$(imap_quote "${test_email}")"
  quoted_password="$(imap_quote "${test_password}")"
  server_name="$(tls_server_name)"
  output_file="$(mktemp)"

  timeout 20 openssl s_client -quiet -crlf \
    -connect "${HOST}:${IMAPS_PORT}" \
    -servername "${server_name}" >"${output_file}" 2>&1 <<EOF || openssl_status=$?
A1 CAPABILITY
A2 LOGIN ${quoted_email} ${quoted_password}
A3 ID ("name" "Microsoft Outlook" "version" "16.0")
A4 NAMESPACE
A5 LIST "" "*"
A6 STATUS INBOX (MESSAGES UIDNEXT UIDVALIDITY UNSEEN)
A7 SELECT INBOX
A8 LOGOUT
EOF

  if ! grep -q '^\* OK LPE IMAP ready' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe did not receive the core IMAP greeting through LPE-CT."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} did not proxy to the core IMAP greeting"
  fi
  if ! grep -q '^A1 OK CAPABILITY completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe reached a greeting but CAPABILITY did not complete."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed CAPABILITY through the proxy"
  fi
  if ! grep -q '^A2 OK LOGIN completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe reached the core IMAP adapter, but LOGIN failed for the supplied test account."
    echo "[DIAG] Re-check mailbox credentials and inspect both lpe-ct.service and lpe.service logs."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed authenticated LOGIN through the proxy"
  fi
  if ! grep -q '^A3 OK ID completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe logged in but Outlook-style ID did not complete."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed Outlook-style ID through the proxy"
  fi
  if ! grep -q '^\* NAMESPACE ' "${output_file}" \
    || ! grep -q '^A4 OK NAMESPACE completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe logged in but NAMESPACE did not complete."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed NAMESPACE through the proxy"
  fi
  if ! grep -q '^\* LIST ' "${output_file}" \
    || ! grep -q '^A5 OK LIST completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe logged in but LIST did not return folders."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed LIST through the proxy"
  fi
  if ! grep -q '^\* STATUS "INBOX"' "${output_file}" \
    || ! grep -q '^A6 OK STATUS completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe logged in but STATUS INBOX did not complete."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed STATUS INBOX through the proxy"
  fi
  if ! grep -q '^A7 OK \[READ-WRITE\] SELECT completed' "${output_file}"; then
    echo "[DIAG] Public IMAPS probe logged in but could not SELECT INBOX."
    recent_logs
    rm -f "${output_file}"
    fail "Public IMAPS on ${HOST}:${IMAPS_PORT} failed SELECT INBOX through the proxy"
  fi

  if [[ "${openssl_status}" -ne 0 ]]; then
    warn "Authenticated public IMAPS probe succeeded, but openssl exited with status ${openssl_status} after the IMAP session closed."
  fi
  rm -f "${output_file}"
  pass "Public IMAPS on ${HOST}:${IMAPS_PORT} accepted Outlook-style login, folder discovery, STATUS, and SELECT INBOX through LPE-CT"
}

probe_client_publication() {
  local base_url="https://${HOST}:${HTTPS_PORT}"
  local host_header="${LPE_CT_PUBLICATION_TEST_HOST:-${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_SERVER_NAME:-localhost}}}"
  local autodiscover_email="${LPE_CT_AUTODISCOVER_TEST_EMAIL:-${LPE_CT_BOOTSTRAP_ADMIN_EMAIL:-admin@example.test}}"
  local expected_imap_host="${LPE_CT_EXPECTED_AUTODISCOVER_IMAP_HOST:-${LPE_AUTOCONFIG_IMAP_HOST:-${host_header}}}"
  local expected_imap_port="${LPE_CT_EXPECTED_AUTODISCOVER_IMAP_PORT:-${LPE_AUTOCONFIG_IMAP_PORT:-${IMAPS_PORT:-993}}}"
  local expected_smtp_host="${LPE_CT_EXPECTED_AUTODISCOVER_SMTP_HOST:-${LPE_AUTOCONFIG_SMTP_HOST:-${expected_imap_host}}}"
  local expected_smtp_port="${LPE_CT_EXPECTED_AUTODISCOVER_SMTP_PORT:-${LPE_AUTOCONFIG_SMTP_PORT:-${SUBMISSION_PORT:-465}}}"
  local body
  local headers_file

  body="$(curl --silent --show-error --fail --insecure \
    --header "Host: ${host_header}" \
    --header 'Content-Type: application/xml' \
    --data "<?xml version=\"1.0\" encoding=\"utf-8\"?><Autodiscover><Request><EMailAddress>${autodiscover_email}</EMailAddress></Request></Autodiscover>" \
    "${base_url}/autodiscover/autodiscover.xml")" \
    || fail "Autodiscover POST is not reachable through LPE-CT HTTPS publication"
  [[ "$body" == *"<Type>IMAP</Type>"* ]] \
    || fail "Autodiscover POST did not publish IMAP through LPE-CT"
  if outlook_scope_enabled; then
    [[ "$body" == *"<Server>${expected_imap_host}</Server>"* ]] \
      || fail "Outlook Autodiscover IMAP server is not ${expected_imap_host}"
    [[ "$body" == *"<Port>${expected_imap_port}</Port>"* ]] \
      || fail "Outlook Autodiscover IMAP port is not ${expected_imap_port}"
    [[ "$body" == *"<SSL>on</SSL>"* ]] \
      || fail "Outlook Autodiscover IMAP profile does not enable SSL"
    [[ "$body" == *"<LoginName>${autodiscover_email}</LoginName>"* ]] \
      || fail "Outlook Autodiscover IMAP login name is not ${autodiscover_email}"
    if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
      [[ "$body" == *"<Type>SMTP</Type>"* ]] \
        || fail "Outlook Autodiscover does not publish SMTP even though LPE-CT submission is configured"
      [[ "$body" == *"<Server>${expected_smtp_host}</Server>"* ]] \
        || fail "Outlook Autodiscover SMTP server is not ${expected_smtp_host}"
      [[ "$body" == *"<Port>${expected_smtp_port}</Port>"* ]] \
        || fail "Outlook Autodiscover SMTP port is not ${expected_smtp_port}"
      [[ "$body" == *"<AuthRequired>on</AuthRequired>"* ]] \
        || fail "Outlook Autodiscover SMTP profile does not require authentication"
      [[ "$body" == *"<UsePOPAuth>off</UsePOPAuth>"* ]] \
        || fail "Outlook Autodiscover SMTP profile does not disable POP-before-SMTP authentication"
      [[ "$body" == *"<SMTPLast>off</SMTPLast>"* ]] \
        || fail "Outlook Autodiscover SMTP profile does not disable SMTP-after-download"
    else
      warn "Outlook Autodiscover SMTP profile was not required because LPE_CT_SUBMISSION_BIND_ADDRESS is not configured."
    fi
    if [[ "${LPE_CT_EXPECTED_OUTLOOK_EXCHANGE_AUTODISCOVER:-false}" != "true" ]]; then
      [[ "$body" != *"<Type>EXCH</Type>"* ]] \
        || fail "Outlook Autodiscover publishes EXCH and may make Outlook choose the unfinished Exchange route instead of IMAP"
      [[ "$body" != *"<Type>EXPR</Type>"* ]] \
        || fail "Outlook Autodiscover publishes EXPR and may make Outlook choose the unfinished Exchange route instead of IMAP"
      [[ "$body" != *"<Type>WEB</Type>"* ]] \
        || fail "Outlook Autodiscover publishes WEB and may make Outlook choose an Exchange-style route instead of IMAP"
      [[ "$body" != *"mapiHttp"* ]] \
        || fail "Outlook Autodiscover publishes mapiHttp in the default IMAP account setup path"
    fi
  fi
  pass "Autodiscover POST publishes IMAP through LPE-CT"

  if ! explicit_scope_enabled "https"; then
    echo "[SKIP] ActiveSync and MAPI publication checks skipped by LPE_CT_EDGE_TEST_SCOPE=${TEST_SCOPE}."
    return 0
  fi

  headers_file="$(mktemp)"
  curl --silent --show-error --fail --insecure --http1.1 \
    --request OPTIONS \
    --header "Host: ${host_header}" \
    --dump-header "${headers_file}" \
    --output /dev/null \
    "${base_url}/Microsoft-Server-ActiveSync" \
    || {
      rm -f "${headers_file}"
      fail "ActiveSync OPTIONS is not reachable through LPE-CT HTTPS publication"
    }
  grep -qi '^ms-asprotocolversions:' "${headers_file}" \
    || {
      sed -n '1,40p' "${headers_file}" || true
      rm -f "${headers_file}"
      fail "ActiveSync OPTIONS response is missing ms-asprotocolversions"
    }
  grep -qi '^ms-asprotocolcommands:' "${headers_file}" \
    || {
      sed -n '1,40p' "${headers_file}" || true
      rm -f "${headers_file}"
      fail "ActiveSync OPTIONS response is missing ms-asprotocolcommands"
    }
  rm -f "${headers_file}"
  pass "ActiveSync OPTIONS exposes ms-asprotocolversions and ms-asprotocolcommands through LPE-CT"

  headers_file="$(mktemp)"
  curl --silent --show-error --fail --insecure --http1.1 \
    --request OPTIONS \
    --header "Host: ${host_header}" \
    --dump-header "${headers_file}" \
    --output /dev/null \
    "${base_url}/mapi/emsmdb" \
    || {
      rm -f "${headers_file}"
      fail "MAPI EMSMDB OPTIONS is not reachable through LPE-CT HTTPS publication"
    }
  grep -qi '^x-lpe-mapi-status: transport-session-ready' "${headers_file}" \
    || {
      sed -n '1,40p' "${headers_file}" || true
      rm -f "${headers_file}"
      fail "MAPI EMSMDB OPTIONS response is missing transport-session-ready status"
    }
  rm -f "${headers_file}"
  pass "MAPI EMSMDB OPTIONS is published through LPE-CT"
}

SMTP_BIND="${LPE_CT_SMTP_BIND_ADDRESS:-}"
if [[ -n "${LPE_CT_SMTP_PORT:-}" ]]; then
  SMTP_PORT="${LPE_CT_SMTP_PORT}"
elif [[ -n "${SMTP_BIND}" && "${SMTP_BIND}" == *:* ]]; then
  SMTP_PORT="${SMTP_BIND##*:}"
else
  SMTP_PORT="25"
fi
HTTPS_PORT="${LPE_CT_NGINX_LISTEN_PORT:-443}"
SUBMISSION_BIND="${LPE_CT_SUBMISSION_BIND_ADDRESS:-}"
IMAPS_BIND="${LPE_CT_IMAPS_BIND_ADDRESS:-}"
SUBMISSION_PORT="${SUBMISSION_BIND##*:}"
IMAPS_PORT="${IMAPS_BIND##*:}"

check_tls_file_readable "public TLS certificate" "${LPE_CT_PUBLIC_TLS_CERT_PATH:-}"
check_tls_file_readable "public TLS private key" "${LPE_CT_PUBLIC_TLS_KEY_PATH:-}"
if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
  check_tls_file_readable "submission TLS certificate" "${LPE_CT_SUBMISSION_TLS_CERT_PATH:-${LPE_CT_PUBLIC_TLS_CERT_PATH:-}}"
  check_tls_file_readable "submission TLS private key" "${LPE_CT_SUBMISSION_TLS_KEY_PATH:-${LPE_CT_PUBLIC_TLS_KEY_PATH:-}}"
fi
if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
  check_tls_file_readable "IMAPS TLS certificate" "${LPE_CT_IMAPS_TLS_CERT_PATH:-${LPE_CT_PUBLIC_TLS_CERT_PATH:-}}"
  check_tls_file_readable "IMAPS TLS private key" "${LPE_CT_IMAPS_TLS_KEY_PATH:-${LPE_CT_PUBLIC_TLS_KEY_PATH:-}}"
fi

if scope_enabled "smtp"; then
  tcp_probe "SMTP ingress" "$HOST" "${SMTP_PORT:-25}"
  if [[ -n "${LPE_CT_PUBLIC_TLS_CERT_PATH:-}" && -n "${LPE_CT_PUBLIC_TLS_KEY_PATH:-}" ]]; then
    smtp_starttls_probe_if_possible "$HOST" "${SMTP_PORT:-25}"
  else
    echo "[SKIP] LPE_CT_PUBLIC_TLS_CERT_PATH and LPE_CT_PUBLIC_TLS_KEY_PATH are not both configured; SMTP STARTTLS is intentionally not advertised."
  fi
else
  echo "[SKIP] SMTP ingress checks skipped by LPE_CT_EDGE_TEST_SCOPE=${TEST_SCOPE}."
fi

if scope_enabled "https"; then
  curl --silent --show-error --fail --insecure "https://${HOST}:${HTTPS_PORT}/" >/dev/null \
    || fail "HTTPS management edge is not reachable on ${HOST}:${HTTPS_PORT}"
  pass "HTTPS management edge is reachable on ${HOST}:${HTTPS_PORT}"
  headers_file="$(mktemp)"
  curl --silent --show-error --fail --insecure --http1.1 \
    --header "Host: ${LPE_CT_PUBLICATION_TEST_HOST:-${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_SERVER_NAME:-localhost}}}" \
    --dump-header "${headers_file}" \
    --output /dev/null \
    "https://${HOST}:${HTTPS_PORT}/" \
    || {
      rm -f "${headers_file}"
      fail "HTTPS security-header probe failed on ${HOST}:${HTTPS_PORT}"
    }
  assert_https_security_headers "${headers_file}" "LPE-CT HTTPS management edge"
  rm -f "${headers_file}"
  pass "HTTPS management edge exposes required security headers"
  tls_probe_if_possible "$HOST" "$HTTPS_PORT" "HTTPS"
  tls_trust_probe_if_possible "$HOST" "$HTTPS_PORT" "HTTPS"
  probe_client_publication
else
  echo "[SKIP] HTTPS publication checks skipped by LPE_CT_EDGE_TEST_SCOPE=${TEST_SCOPE}."
fi

if scope_enabled "submission"; then
  if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
    tls_probe_if_possible "$HOST" "$SUBMISSION_PORT" "SMTPS submission"
    tls_trust_probe_if_possible "$HOST" "$SUBMISSION_PORT" "SMTPS submission"
    probe_submission_auth_if_configured
  else
    echo "[SKIP] LPE_CT_SUBMISSION_BIND_ADDRESS is not configured; port 465 is intentionally not enabled."
  fi
else
  echo "[SKIP] SMTPS submission checks skipped by LPE_CT_EDGE_TEST_SCOPE=${TEST_SCOPE}."
fi

if scope_enabled "imaps"; then
  if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
    tls_probe_if_possible "$HOST" "$IMAPS_PORT" "IMAPS"
    tls_trust_probe_if_possible "$HOST" "$IMAPS_PORT" "IMAPS"
    probe_imaps_upstream "${LPE_CT_IMAPS_UPSTREAM_ADDRESS:-}"
    probe_public_imaps_auth_if_configured
  else
    echo "[SKIP] LPE_CT_IMAPS_BIND_ADDRESS is not configured; port 993 is not enabled."
  fi
else
  echo "[SKIP] IMAPS checks skipped by LPE_CT_EDGE_TEST_SCOPE=${TEST_SCOPE}."
fi

echo "LPE-CT edge port test completed successfully."
