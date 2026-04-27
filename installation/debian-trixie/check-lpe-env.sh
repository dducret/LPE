#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
EXAMPLE_FILE="${EXAMPLE_FILE:-${SCRIPT_DIR}/lpe.env.example}"
STRICT=false
APPEND_MISSING=false

usage() {
  cat <<'USAGE'
Usage: check-lpe-env.sh [options]

Compares an LPE environment file with lpe.env.example and prints the KEY=value
lines that are missing from the deployed file.

Options:
  --env-file PATH       Environment file to inspect. Default: /etc/lpe/lpe.env
  --example-file PATH   Reference example file. Default: sibling lpe.env.example
  --append-missing      Append missing KEY=value lines to the environment file
  --strict              Exit with status 1 when any variable is missing
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --example-file)
      EXAMPLE_FILE="${2:-}"
      shift 2
      ;;
    --append-missing)
      APPEND_MISSING=true
      shift
      ;;
    --strict)
      STRICT=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${ENV_FILE}" || -z "${EXAMPLE_FILE}" ]]; then
  echo "[FAIL] --env-file and --example-file must not be empty." >&2
  exit 2
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[FAIL] Environment file not found: ${ENV_FILE}" >&2
  exit 2
fi

if [[ ! -f "${EXAMPLE_FILE}" ]]; then
  echo "[FAIL] Example environment file not found: ${EXAMPLE_FILE}" >&2
  exit 2
fi

extract_env_lines() {
  local file="$1"
  awk '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*=/ {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      key = line
      sub(/=.*/, "", key)
      if (!(key in seen)) {
        seen[key] = line
        order[++count] = key
      }
    }
    END {
      for (i = 1; i <= count; i++) {
        key = order[i]
        print key "\t" seen[key]
      }
    }
  ' "${file}"
}

declare -A current_keys=()
while IFS=$'\t' read -r key _line; do
  [[ -n "${key}" ]] || continue
  current_keys["${key}"]=1
done < <(extract_env_lines "${ENV_FILE}")

missing_lines=()
while IFS=$'\t' read -r key line; do
  [[ -n "${key}" ]] || continue
  if [[ -z "${current_keys[${key}]:-}" ]]; then
    missing_lines+=("${line}")
  fi
done < <(extract_env_lines "${EXAMPLE_FILE}")

if [[ ${#missing_lines[@]} -eq 0 ]]; then
  echo "[OK] ${ENV_FILE} contains every active variable from ${EXAMPLE_FILE}."
  exit 0
fi

echo "[WARN] ${ENV_FILE} is missing ${#missing_lines[@]} active variable(s) from ${EXAMPLE_FILE}:"
printf '%s\n' "${missing_lines[@]}"

if [[ "${APPEND_MISSING}" == "true" ]]; then
  {
    echo
    echo "# Added from $(basename "${EXAMPLE_FILE}") by check-lpe-env.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "${missing_lines[@]}"
  } >> "${ENV_FILE}"
  echo "[OK] Missing variables were appended to ${ENV_FILE}."
fi

if [[ "${STRICT}" == "true" ]]; then
  exit 1
fi

exit 0
