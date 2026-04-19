#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-}"
ROLE_FILE="${2:-${LPE_HA_ROLE_FILE:-/var/lib/lpe/ha-role}}"

case "${ROLE}" in
  active|standby|drain|maintenance)
    ;;
  *)
    echo "Usage: $0 {active|standby|drain|maintenance} [role-file]" >&2
    exit 1
    ;;
esac

install -d -m 0750 "$(dirname "${ROLE_FILE}")"
tmp_file="$(mktemp "${ROLE_FILE}.tmp.XXXXXX")"
printf '%s\n' "${ROLE}" > "${tmp_file}"
chmod 0640 "${tmp_file}"
mv -f "${tmp_file}" "${ROLE_FILE}"

echo "LPE HA role set to ${ROLE} in ${ROLE_FILE}"
