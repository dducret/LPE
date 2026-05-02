#!/usr/bin/env bash
set -euo pipefail

TIMESYNCD_DROP_IN="/etc/systemd/timesyncd.conf.d/lpe-ct.conf"

usage() {
  cat >&2 <<'EOF'
Usage: lpe-ct-host-action <action>

Actions:
  ntp-update <true|false>   Read NTP servers from stdin and update systemd-timesyncd.
  ntp-sync                  Enable NTP and restart systemd-timesyncd.
  apt-upgrade               Run apt update and apt upgrade -y.
  restart                   Request a host restart.
  shutdown                  Request a host shutdown.
EOF
}

validate_ntp_server() {
  local server="$1"
  [[ -n "${server}" ]] || return 1
  [[ ${#server} -le 253 ]] || return 1
  [[ "${server}" =~ ^[A-Za-z0-9._:-]+$ ]]
}

read_ntp_servers() {
  local line
  local server
  local -a servers=()

  while IFS= read -r line; do
    for server in ${line}; do
      server="${server%,}"
      [[ -n "${server}" ]] || continue
      validate_ntp_server "${server}" || {
        echo "Unsupported NTP server value: ${server}" >&2
        return 1
      }
      servers+=("${server}")
    done
  done

  printf '%s\n' "${servers[@]}"
}

write_timesyncd_config() {
  local enabled="$1"
  local parent
  local temp_file
  local -a servers=()

  mapfile -t servers < <(read_ntp_servers)
  if [[ "${enabled}" == "true" && ${#servers[@]} -eq 0 ]]; then
    echo "At least one NTP server is required when NTP is enabled." >&2
    return 1
  fi

  parent="$(dirname "${TIMESYNCD_DROP_IN}")"
  install -d -m 0755 -o root -g root "${parent}"
  temp_file="$(mktemp "${parent}/lpe-ct.conf.XXXXXX")"
  trap 'rm -f "${temp_file}"' RETURN
  {
    printf '# Managed by LPE-CT management console.\n'
    printf '[Time]\n'
    printf 'NTP=%s\n' "${servers[*]}"
  } > "${temp_file}"
  chown root:root "${temp_file}"
  chmod 0644 "${temp_file}"
  mv "${temp_file}" "${TIMESYNCD_DROP_IN}"
  trap - RETURN
}

ntp_update() {
  local enabled="${1:-}"
  case "${enabled}" in
    true|false) ;;
    *)
      echo "ntp-update requires true or false." >&2
      return 2
      ;;
  esac

  write_timesyncd_config "${enabled}"
  if [[ "${enabled}" == "true" ]]; then
    timedatectl set-ntp true
    systemctl enable --now systemd-timesyncd
    systemctl restart systemd-timesyncd
  else
    timedatectl set-ntp false
    systemctl disable --now systemd-timesyncd
  fi
}

ntp_sync() {
  timedatectl set-ntp true
  systemctl enable --now systemd-timesyncd
  systemctl restart systemd-timesyncd
  timedatectl timesync-status || true
}

apt_upgrade() {
  export DEBIAN_FRONTEND=noninteractive
  apt update
  apt upgrade -y
}

run_outside_service_sandbox() {
  if [[ "${LPE_CT_HOST_ACTION_IN_SYSTEMD:-}" == "1" ]]; then
    return 0
  fi

  # The web service is sandboxed; run the root operation in a fresh unit so
  # apt, timesyncd, reboot, and poweroff are not trapped in its mount namespace.
  exec systemd-run \
    --quiet \
    --pipe \
    --wait \
    --collect \
    --service-type=exec \
    --setenv=LPE_CT_HOST_ACTION_IN_SYSTEMD=1 \
    "$0" "$@"
}

main() {
  local action="${1:-}"
  shift || true

  run_outside_service_sandbox "${action}" "$@"

  case "${action}" in
    ntp-update)
      ntp_update "$@"
      ;;
    ntp-sync)
      ntp_sync
      ;;
    apt-upgrade)
      apt_upgrade
      ;;
    restart)
      systemctl reboot
      ;;
    shutdown)
      systemctl poweroff
      ;;
    -h|--help|"")
      usage
      [[ -n "${action}" ]]
      ;;
    *)
      echo "Unsupported host action: ${action}" >&2
      usage
      return 2
      ;;
  esac
}

main "$@"
