#!/usr/bin/env bash

trim() {
  local value="${1-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

is_interactive_install() {
  if [[ "${INSTALL_FORCE_INTERACTIVE:-}" == "1" ]]; then
    return 0
  fi

  if [[ "${INSTALL_NONINTERACTIVE:-}" == "1" ]]; then
    return 1
  fi

  if [[ -e /dev/tty && -r /dev/tty && -w /dev/tty ]]; then
    return 0
  fi

  [[ -t 0 || -t 2 ]]
}

prompt_output_target() {
  if [[ -e /dev/tty && -w /dev/tty ]]; then
    printf '/dev/tty'
    return 0
  fi

  printf '/dev/stderr'
}

prompt_input_target() {
  if [[ -e /dev/tty && -r /dev/tty ]]; then
    printf '/dev/tty'
    return 0
  fi

  printf '/dev/stdin'
}

prompt_print() {
  local target
  target="$(prompt_output_target)"
  printf '%s' "$*" > "${target}"
}

prompt_println() {
  local target
  target="$(prompt_output_target)"
  printf '%s\n' "$*" > "${target}"
}

prompt_read_line() {
  local __resultvar="$1"
  local -n __resultref="${__resultvar}"
  local input_target
  local input_value=""

  input_target="$(prompt_input_target)"
  IFS= read -r input_value < "${input_target}" || true
  __resultref="${input_value}"
}

prompt_read_secret() {
  local __resultvar="$1"
  local -n __resultref="${__resultvar}"
  local input_target
  local output_target
  local input_value=""

  input_target="$(prompt_input_target)"
  output_target="$(prompt_output_target)"
  IFS= read -r -s input_value < "${input_target}" || true
  printf '\n' > "${output_target}"
  __resultref="${input_value}"
}

print_section() {
  local title="$1"
  if is_interactive_install; then
    prompt_println ""
    prompt_println "${title}"
  fi
}

fail_install() {
  echo "${1}" >&2
  exit 1
}

load_env_file_if_present() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${file}"
    set +a
  fi
}

shell_quote() {
  printf '%q' "${1}"
}

write_env_value() {
  local file="$1"
  local key="$2"
  local value="$3"
  local line
  local temp_file="/tmp/lpe-install-common.$$"

  printf -v line '%s=%q' "${key}" "${value}"
  mkdir -p "$(dirname "${file}")"
  touch "${file}"
  rm -f "${temp_file}"

  awk -v key="${key}" -v line="${line}" '
    BEGIN { written = 0 }
    index($0, key "=") == 1 {
      if (!written) {
        print line
        written = 1
      }
      next
    }
    { print }
    END {
      if (!written) {
        print line
      }
    }
  ' "${file}" > "${temp_file}"

  mv "${temp_file}" "${file}"
}

render_template() {
  local template_file="$1"
  local output_file="$2"
  shift 2
  local perl_script='
    my %vars = map { split(/=/, $_, 2) } @ARGV;
    local $/ = undef;
    my $content = <STDIN>;
    $content =~ s/__([A-Z0-9_]+)__/exists $vars{$1} ? $vars{$1} : "__${1}__"/ge;
    print $content;
  '

  mkdir -p "$(dirname "${output_file}")"
  perl -e "${perl_script}" "$@" < "${template_file}" > "${output_file}"
}

normalize_yes_no() {
  local value
  value="$(trim "${1-}")"
  value="${value,,}"
  case "${value}" in
    y|yes|true|1|on)
      printf 'yes'
      return 0
      ;;
    n|no|false|0|off)
      printf 'no'
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ask_with_default() {
  local label="$1"
  local default_value="${2-}"
  local validator="${3-}"
  local error_message="${4:-Invalid value.}"
  local value

  if ! is_interactive_install; then
    if [[ -z "${default_value}" ]]; then
      fail_install "${label} is required in non-interactive mode."
    fi

    value="$(trim "${default_value}")"
    if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
      fail_install "${error_message}"
    fi

    printf '%s' "${value}"
    return 0
  fi

  while true; do
    prompt_print "${label} (${default_value}): "
    prompt_read_line value
    value="$(trim "${value}")"
    if [[ -z "${value}" ]]; then
      value="$(trim "${default_value}")"
    fi

    if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
      prompt_println "${error_message}"
      continue
    fi

    printf '%s' "${value}"
    return 0
  done
}

ask_required() {
  local label="$1"
  local default_value="${2-}"
  local validator="${3-}"
  local error_message="${4:-This value is required.}"
  local value

  if ! is_interactive_install; then
    if [[ -n "${default_value}" ]]; then
      value="$(trim "${default_value}")"
      if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
        fail_install "${error_message}"
      fi

      printf '%s' "${value}"
      return 0
    fi

    fail_install "${label} is required in non-interactive mode."
  fi

  while true; do
    if [[ -n "${default_value}" ]]; then
      prompt_print "${label} (${default_value}) (required): "
    else
      prompt_print "${label} (required): "
    fi

    prompt_read_line value
    value="$(trim "${value}")"

    if [[ -z "${value}" ]]; then
      if [[ -n "${default_value}" ]]; then
        value="$(trim "${default_value}")"
      else
        prompt_println "${error_message}"
        continue
      fi
    fi

    if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
      prompt_println "${error_message}"
      continue
    fi

    printf '%s' "${value}"
    return 0
  done
}

ask_secret_with_default_behavior_when_possible() {
  local label="$1"
  local default_value="${2-}"
  local validator="${3-}"
  local error_message="${4:-Invalid value.}"
  local prompt_suffix
  local value

  if ! is_interactive_install; then
    if [[ -z "${default_value}" ]]; then
      fail_install "${label} is required in non-interactive mode."
    fi

    value="$(trim "${default_value}")"
    if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
      fail_install "${error_message}"
    fi

    printf '%s' "${value}"
    return 0
  fi

  if [[ -n "${default_value}" ]]; then
    prompt_suffix="current value retained on Enter"
  else
    prompt_suffix="required"
  fi

  while true; do
    prompt_print "${label} (${prompt_suffix}): "
    prompt_read_secret value
    value="$(trim "${value}")"

    if [[ -z "${value}" ]]; then
      value="${default_value}"
    fi

    if [[ -z "${value}" ]]; then
      prompt_println "${error_message}"
      continue
    fi

    if [[ -n "${validator}" ]] && ! "${validator}" "${value}"; then
      prompt_println "${error_message}"
      continue
    fi

    printf '%s' "${value}"
    return 0
  done
}

ask_yes_no() {
  local label="$1"
  local default_value="${2:-yes}"
  local candidate="${3-}"
  local normalized_default
  local normalized_candidate
  local value

  normalized_default="$(normalize_yes_no "${default_value}")" || fail_install "Invalid yes/no default for ${label}."

  if [[ -n "${candidate}" ]]; then
    normalized_candidate="$(normalize_yes_no "${candidate}")" || fail_install "Invalid yes/no value for ${label}."
    candidate="${normalized_candidate}"
  fi

  if ! is_interactive_install; then
    if [[ -n "${candidate}" ]]; then
      printf '%s' "${candidate}"
    else
      printf '%s' "${normalized_default}"
    fi
    return 0
  fi

  while true; do
    prompt_print "${label} (${normalized_default}): "
    prompt_read_line value
    value="$(trim "${value}")"

    if [[ -z "${value}" ]]; then
      if [[ -n "${candidate}" ]]; then
        printf '%s' "${candidate}"
      else
        printf '%s' "${normalized_default}"
      fi
      return 0
    fi

    if normalize_yes_no "${value}" >/dev/null; then
      normalize_yes_no "${value}"
      return 0
    fi

    prompt_println "Enter yes or no."
  done
}

validate_nonempty() {
  [[ -n "$(trim "${1-}")" ]]
}

validate_port() {
  local value="$1"
  [[ "${value}" =~ ^[0-9]+$ ]] || return 1
  (( value >= 1 && value <= 65535 ))
}

validate_hostname() {
  local value="$1"
  [[ "${value}" =~ ^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$ ]]
}

validate_host_token() {
  local value="$1"
  [[ "${value}" =~ ^[A-Za-z0-9._-]+$ ]]
}

validate_email() {
  local value="$1"
  [[ "${value}" =~ ^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$ ]]
}

validate_directory_path() {
  local value="$1"
  [[ "${value}" == /* ]]
}

validate_exact_path() {
  local expected="$1"
  local actual="$2"
  [[ "${actual}" == "${expected}" ]]
}

validate_http_url() {
  local value="$1"
  [[ "${value}" =~ ^https?://[^[:space:]]+$ ]]
}

validate_smtp_url() {
  local value="$1"
  [[ "${value}" =~ ^smtp://[^[:space:]]+$ ]]
}

validate_password_nonempty() {
  [[ -n "$(trim "${1-}")" ]]
}

validate_shared_secret() {
  local value="$1"
  local lowered
  lowered="${value,,}"
  [[ ${#value} -ge 32 ]] || return 1
  case "${lowered}" in
    change-me|changeme|shared-secret|integration-test|password|default|test|example)
      return 1
      ;;
  esac
  return 0
}

urlencode() {
  local raw="${1}"
  local length="${#raw}"
  local encoded=""
  local index
  local char

  for (( index = 0; index < length; index++ )); do
    char="${raw:index:1}"
    case "${char}" in
      [a-zA-Z0-9.~_-])
        encoded+="${char}"
        ;;
      *)
        printf -v char '%%%02X' "'${char}"
        encoded+="${char}"
        ;;
    esac
  done

  printf '%s' "${encoded}"
}

build_postgres_url() {
  local host="$1"
  local port="$2"
  local database="$3"
  local username="$4"
  local password="$5"
  local encoded_username
  local encoded_password
  local encoded_database

  encoded_username="$(urlencode "${username}")"
  encoded_password="$(urlencode "${password}")"
  encoded_database="$(urlencode "${database}")"
  printf 'postgres://%s:%s@%s:%s/%s' \
    "${encoded_username}" \
    "${encoded_password}" \
    "${host}" \
    "${port}" \
    "${encoded_database}"
}

derive_database_url_from_env() {
  local host="${LPE_DB_HOST:-}"
  local port="${LPE_DB_PORT:-}"
  local database="${LPE_DB_NAME:-}"
  local username="${LPE_DB_USER:-}"
  local password="${LPE_DB_PASSWORD:-}"

  [[ -n "${host}" ]] || return 1
  [[ -n "${port}" ]] || return 1
  [[ -n "${database}" ]] || return 1
  [[ -n "${username}" ]] || return 1
  [[ -n "${password}" ]] || return 1

  build_postgres_url "${host}" "${port}" "${database}" "${username}" "${password}"
}

ensure_database_url() {
  if [[ -n "${DATABASE_URL:-}" ]]; then
    return 0
  fi

  DATABASE_URL="$(derive_database_url_from_env)" || return 1
  export DATABASE_URL
}

format_public_url() {
  local scheme="$1"
  local host="$2"
  local port="$3"
  local path="${4:-}"

  if [[ "${port}" == "80" && "${scheme}" == "http" ]] || [[ "${port}" == "443" && "${scheme}" == "https" ]]; then
    printf '%s://%s%s' "${scheme}" "${host}" "${path}"
    return 0
  fi

  printf '%s://%s:%s%s' "${scheme}" "${host}" "${port}" "${path}"
}
