#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

BIND_ADDRESS="${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}"
BODY="$(curl --silent --show-error --fail "http://${BIND_ADDRESS}/health/ready")"

if [[ "${BODY}" == *'"status":"ready"'* ]]; then
  exit 0
fi

echo "${BODY}" >&2
exit 1
