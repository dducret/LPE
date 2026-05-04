#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
HOST="${HOST:-127.0.0.1}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"

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

[[ -f "$ENV_FILE" ]] || fail "Environment file not found: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

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
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; TCP reachability for ${name} was checked, TLS handshake was not."
    return
  fi
  output_file="$(mktemp)"
  timeout 10 openssl s_client -showcerts -connect "${host}:${port}" -servername "${LPE_CT_PUBLIC_HOSTNAME:-localhost}" </dev/null >"${output_file}" 2>&1 || openssl_status=$?
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

smtp_starttls_probe_if_possible() {
  local host="$1"
  local port="$2"
  local output_file
  local openssl_status=0
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; SMTP STARTTLS handshake was not checked."
    return
  fi
  output_file="$(mktemp)"
  timeout 10 openssl s_client -starttls smtp -crlf -showcerts -connect "${host}:${port}" -servername "${LPE_CT_PUBLIC_HOSTNAME:-localhost}" </dev/null >"${output_file}" 2>&1 || openssl_status=$?
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

probe_client_publication() {
  local base_url="https://${HOST}:${HTTPS_PORT}"
  local host_header="${LPE_CT_PUBLICATION_TEST_HOST:-${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_SERVER_NAME:-localhost}}}"
  local autodiscover_email="${LPE_CT_AUTODISCOVER_TEST_EMAIL:-${LPE_CT_BOOTSTRAP_ADMIN_EMAIL:-admin@example.test}}"
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
  pass "Autodiscover POST publishes IMAP through LPE-CT"

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

tcp_probe "SMTP ingress" "$HOST" "${SMTP_PORT:-25}"
if [[ -n "${LPE_CT_PUBLIC_TLS_CERT_PATH:-}" && -n "${LPE_CT_PUBLIC_TLS_KEY_PATH:-}" ]]; then
  smtp_starttls_probe_if_possible "$HOST" "${SMTP_PORT:-25}"
else
  echo "[SKIP] LPE_CT_PUBLIC_TLS_CERT_PATH and LPE_CT_PUBLIC_TLS_KEY_PATH are not both configured; SMTP STARTTLS is intentionally not advertised."
fi

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
probe_client_publication

if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
  tls_probe_if_possible "$HOST" "$SUBMISSION_PORT" "SMTPS submission"
else
  echo "[SKIP] LPE_CT_SUBMISSION_BIND_ADDRESS is not configured; port 465 is intentionally not enabled."
fi

if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
  tls_probe_if_possible "$HOST" "$IMAPS_PORT" "IMAPS"
  probe_imaps_upstream "${LPE_CT_IMAPS_UPSTREAM_ADDRESS:-}"
else
  echo "[SKIP] LPE_CT_IMAPS_BIND_ADDRESS is not configured; port 993 is not enabled."
fi

echo "LPE-CT edge port test completed successfully."
