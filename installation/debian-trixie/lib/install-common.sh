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

mapi_identity_key_constraint_count() {
  local database_url="$1"

  psql "${database_url}" -X -v ON_ERROR_STOP=1 -At <<'SQL'
SELECT COUNT(*)::int
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE n.nspname = 'public'
  AND r.relname = 'mapi_object_identities'
  AND c.contype = 'c'
  AND (
    (
      c.conname = 'mapi_object_identities_source_key_check'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(source_key) = 22%'
    )
    OR (
      c.conname = 'mapi_object_identities_change_key_check'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(change_key) >= 17%'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(change_key) <= 24%'
    )
    OR (
      c.conname = 'mapi_object_identities_instance_key_check'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(instance_key) = 22%'
    )
  );
SQL
}

mapi_calendar_event_move_change_key_constraint_count() {
  local database_url="$1"

  psql "${database_url}" -X -v ON_ERROR_STOP=1 -At <<'SQL'
SELECT COUNT(*)::int
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE n.nspname = 'public'
  AND r.relname = 'mapi_calendar_event_identity_moves'
  AND c.contype = 'c'
  AND (
    (
      pg_get_constraintdef(c.oid) LIKE '%octet_length(old_change_key) >= 17%'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(old_change_key) <= 24%'
    )
    OR (
      pg_get_constraintdef(c.oid) LIKE '%octet_length(new_change_key) >= 17%'
      AND pg_get_constraintdef(c.oid) LIKE '%octet_length(new_change_key) <= 24%'
    )
  );
SQL
}

mapi_special_folder_alias_shape_ok() {
  local database_url="$1"

  psql "${database_url}" -X -v ON_ERROR_STOP=1 -At <<'SQL'
SELECT CASE WHEN
  to_regclass('public.mapi_special_folder_aliases') IS NOT NULL
  AND (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_special_folder_aliases'
      AND column_name IN ('alias_folder_id', 'canonical_folder_id', 'source_key', 'mapi_change_number')
      AND is_nullable = 'NO'
      AND data_type = CASE column_name
        WHEN 'alias_folder_id' THEN 'bigint'
        WHEN 'canonical_folder_id' THEN 'bigint'
        WHEN 'source_key' THEN 'bytea'
        WHEN 'mapi_change_number' THEN 'bigint'
      END
  ) = 4
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%alias_folder_id >= 2818049%'
      AND replace(pg_get_constraintdef(oid), '''', '') LIKE '%alias_folder_id < 9223369837831520257%'
      AND pg_get_constraintdef(oid) LIKE '%65535%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%canonical_folder_id > 0%'
      AND pg_get_constraintdef(oid) LIKE '%canonical_folder_id <= 2752513%'
      AND pg_get_constraintdef(oid) LIKE '%65535%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%octet_length(source_key) = 22%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%mapi_change_number >= 43%'
      AND replace(pg_get_constraintdef(oid), '''', '') LIKE '%mapi_change_number < 140737454800896%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%alias_folder_id <> canonical_folder_id%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'p'
      AND pg_get_constraintdef(oid) = 'PRIMARY KEY (tenant_id, account_id, alias_folder_id)'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'u'
      AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, source_key)'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'u'
      AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, mapi_change_number)'
  )
  AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'u'
      AND pg_get_constraintdef(oid) LIKE '%canonical_folder_id%'
  )
  AND EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
      AND contype = 'f'
      AND pg_get_constraintdef(oid) LIKE '%FOREIGN KEY (tenant_id, account_id)%'
      AND pg_get_constraintdef(oid) LIKE '%REFERENCES accounts(tenant_id, id)%'
      AND pg_get_constraintdef(oid) LIKE '%ON DELETE CASCADE%'
  )
THEN 1 ELSE 0 END;
SQL
}

mapi_local_replica_range_shape_ok() {
  local database_url="$1"

  psql "${database_url}" -X -v ON_ERROR_STOP=1 -At <<'SQL'
WITH expected_columns(table_name, column_name, ordinal_position, data_type, is_nullable, column_default) AS (
  VALUES
    ('mapi_local_replica_id_ranges'::text, 'tenant_id'::text, 1, 'uuid'::text, 'NO'::text, NULL::text),
    ('mapi_local_replica_id_ranges', 'account_id', 2, 'uuid', 'NO', NULL),
    ('mapi_local_replica_id_ranges', 'replica_guid', 3, 'uuid', 'NO', NULL),
    ('mapi_local_replica_id_ranges', 'first_global_counter', 4, 'bigint', 'NO', NULL),
    ('mapi_local_replica_id_ranges', 'end_global_counter_exclusive', 5, 'bigint', 'NO', NULL),
    ('mapi_local_replica_id_ranges', 'created_at', 6, 'timestamp with time zone', 'NO', 'now()'),
    ('mapi_local_replica_deleted_ranges', 'tenant_id', 1, 'uuid', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'account_id', 2, 'uuid', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'folder_id', 3, 'bigint', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'replica_guid', 4, 'uuid', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'min_global_counter', 5, 'bigint', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'max_global_counter', 6, 'bigint', 'NO', NULL),
    ('mapi_local_replica_deleted_ranges', 'created_at', 7, 'timestamp with time zone', 'NO', 'now()')
),
actual_columns AS (
  SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name IN ('mapi_local_replica_id_ranges', 'mapi_local_replica_deleted_ranges')
),
expected_constraints(table_name, constraint_type, definition) AS (
  VALUES
    ('mapi_local_replica_id_ranges'::text, 'c'::text, 'CHECK ((first_global_counter < end_global_counter_exclusive))'::text),
    ('mapi_local_replica_id_ranges', 'c', 'CHECK (((end_global_counter_exclusive > 43) AND (end_global_counter_exclusive <= ''140737454800896''::bigint)))'),
    ('mapi_local_replica_id_ranges', 'c', 'CHECK (((first_global_counter >= 43) AND (first_global_counter < ''140737454800896''::bigint)))'),
    ('mapi_local_replica_id_ranges', 'f', 'FOREIGN KEY (tenant_id, account_id, replica_guid) REFERENCES mapi_mailbox_replicas(tenant_id, account_id, replica_guid) ON DELETE CASCADE'),
    ('mapi_local_replica_id_ranges', 'p', 'PRIMARY KEY (tenant_id, account_id, replica_guid, first_global_counter)'),
    ('mapi_local_replica_deleted_ranges', 'c', 'CHECK ((min_global_counter <= max_global_counter))'),
    ('mapi_local_replica_deleted_ranges', 'c', 'CHECK ((folder_id > 0))'),
    ('mapi_local_replica_deleted_ranges', 'c', 'CHECK (((max_global_counter >= 43) AND (max_global_counter < ''140737454800896''::bigint)))'),
    ('mapi_local_replica_deleted_ranges', 'c', 'CHECK (((min_global_counter >= 43) AND (min_global_counter < ''140737454800896''::bigint)))'),
    ('mapi_local_replica_deleted_ranges', 'f', 'FOREIGN KEY (tenant_id, account_id, replica_guid) REFERENCES mapi_mailbox_replicas(tenant_id, account_id, replica_guid) ON DELETE CASCADE'),
    ('mapi_local_replica_deleted_ranges', 'p', 'PRIMARY KEY (tenant_id, account_id, folder_id, replica_guid, min_global_counter, max_global_counter)')
),
actual_constraints AS (
  SELECT table_row.relname::text AS table_name,
         constraint_row.contype::text AS constraint_type,
         replace(pg_get_constraintdef(constraint_row.oid), 'public.', '') AS definition
  FROM pg_constraint constraint_row
  JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
  JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
  WHERE namespace_row.nspname = 'public'
    AND table_row.relname IN ('mapi_local_replica_id_ranges', 'mapi_local_replica_deleted_ranges')
    AND constraint_row.contype IN ('c', 'f', 'p')
),
expected_indexes(table_name, index_name, definition) AS (
  VALUES
    ('mapi_local_replica_id_ranges'::text, 'mapi_local_replica_id_ranges_membership_idx'::text, 'CREATE INDEX mapi_local_replica_id_ranges_membership_idx ON mapi_local_replica_id_ranges USING btree (tenant_id, account_id, replica_guid, first_global_counter, end_global_counter_exclusive)'::text),
    ('mapi_local_replica_id_ranges', 'mapi_local_replica_id_ranges_pkey', 'CREATE UNIQUE INDEX mapi_local_replica_id_ranges_pkey ON mapi_local_replica_id_ranges USING btree (tenant_id, account_id, replica_guid, first_global_counter)'),
    ('mapi_local_replica_deleted_ranges', 'mapi_local_replica_deleted_ranges_folder_idx', 'CREATE INDEX mapi_local_replica_deleted_ranges_folder_idx ON mapi_local_replica_deleted_ranges USING btree (tenant_id, account_id, folder_id, replica_guid, min_global_counter, max_global_counter)'),
    ('mapi_local_replica_deleted_ranges', 'mapi_local_replica_deleted_ranges_pkey', 'CREATE UNIQUE INDEX mapi_local_replica_deleted_ranges_pkey ON mapi_local_replica_deleted_ranges USING btree (tenant_id, account_id, folder_id, replica_guid, min_global_counter, max_global_counter)')
),
actual_indexes AS (
  SELECT tablename::text AS table_name,
         indexname::text AS index_name,
         replace(indexdef, 'public.', '') AS definition
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND tablename IN ('mapi_local_replica_id_ranges', 'mapi_local_replica_deleted_ranges')
)
SELECT CASE WHEN
  NOT EXISTS (
    (SELECT * FROM expected_columns EXCEPT SELECT * FROM actual_columns)
    UNION ALL
    (SELECT * FROM actual_columns EXCEPT SELECT * FROM expected_columns)
  )
  AND NOT EXISTS (
    (SELECT * FROM expected_constraints EXCEPT SELECT * FROM actual_constraints)
    UNION ALL
    (SELECT * FROM actual_constraints EXCEPT SELECT * FROM expected_constraints)
  )
  AND NOT EXISTS (
    (SELECT * FROM expected_indexes EXCEPT SELECT * FROM actual_indexes)
    UNION ALL
    (SELECT * FROM actual_indexes EXCEPT SELECT * FROM expected_indexes)
  )
THEN 1 ELSE 0 END;
SQL
}

mapi_outlook_cache_fidelity_shape_ok() {
  local database_url="$1"

  psql "${database_url}" -X -v ON_ERROR_STOP=1 -At <<'SQL'
SELECT CASE WHEN
  (
    SELECT COUNT(*) = 6
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_navigation_shortcuts'
      AND (
            (column_name = 'ordinal' AND data_type = 'bytea' AND is_nullable = 'NO' AND column_default IS NULL)
            OR (column_name = 'calendar_color' AND data_type = 'integer' AND is_nullable = 'YES' AND column_default IS NULL)
            OR (column_name = 'address_book_entry_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
            OR (column_name = 'address_book_store_entry_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
            OR (column_name = 'client_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
            OR (column_name = 'ro_group_type' AND data_type = 'integer' AND is_nullable = 'YES' AND column_default IS NULL)
      )
  )
  AND (
    SELECT COUNT(*) = 6
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = to_regclass('public.mapi_navigation_shortcuts')
      AND constraint_row.contype = 'c'
      AND (
            (constraint_row.conname = 'mapi_navigation_shortcuts_ordinal_check'
             AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(ordinal) > 0%'
             AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(ordinal) <= 65535%'
             AND pg_get_constraintdef(constraint_row.oid) LIKE '%get_byte(ordinal, (octet_length(ordinal) - 1)) <> 0%'
             AND pg_get_constraintdef(constraint_row.oid) LIKE '%get_byte(ordinal, (octet_length(ordinal) - 1)) <> 255%')
            OR (constraint_row.conname = 'mapi_navigation_shortcuts_calendar_color_check'
                AND replace(pg_get_constraintdef(constraint_row.oid), '''', '') LIKE '%calendar_color >= -1%'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%calendar_color <= 14%')
            OR (constraint_row.conname = 'mapi_navigation_shortcuts_address_book_entry_id_check'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_entry_id) > 0%'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_entry_id) <= 65535%')
            OR (constraint_row.conname = 'mapi_navigation_shortcuts_address_book_store_entry_id_check'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_store_entry_id) > 0%'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_store_entry_id) <= 65535%')
            OR (constraint_row.conname = 'mapi_navigation_shortcuts_client_id_check'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(client_id) > 0%'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(client_id) <= 65535%')
            OR (constraint_row.conname = 'mapi_navigation_shortcuts_ro_group_type_check'
                AND replace(pg_get_constraintdef(constraint_row.oid), '''', '') LIKE '%ro_group_type >= -1%'
                AND pg_get_constraintdef(constraint_row.oid) LIKE '%ro_group_type <= 4%')
      )
  )
  AND EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'mapi_navigation_shortcuts'
      AND indexname = 'mapi_navigation_shortcuts_account_idx'
      AND replace(indexdef, 'public.', '') =
          'CREATE INDEX mapi_navigation_shortcuts_account_idx ON mapi_navigation_shortcuts USING btree (tenant_id, account_id, section, ordinal, subject, id)'
  )
  AND EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'mapi_associated_config_messages'
      AND indexname = 'mapi_associated_config_messages_logical_idx'
      AND replace(indexdef, 'public.', '') =
          'CREATE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)'
  )
THEN 1 ELSE 0 END;
SQL
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
  local -a validator_args=()

  if [[ -n "${validator}" ]]; then
    # Split validator function and its fixed arguments, for cases like
    # "validate_exact_path /opt/lpe".
    read -r -a validator_args <<< "${validator}"
  fi

  if ! is_interactive_install; then
    if [[ -z "${default_value}" ]]; then
      fail_install "${label} is required in non-interactive mode."
    fi

    value="$(trim "${default_value}")"
    if [[ ${#validator_args[@]} -gt 0 ]] && ! "${validator_args[@]}" "${value}"; then
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

    if [[ ${#validator_args[@]} -gt 0 ]] && ! "${validator_args[@]}" "${value}"; then
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

validate_https_port() {
  local value="$1"
  validate_port "${value}" && [[ "${value}" != "80" ]]
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

validate_absolute_file_path() {
  local value="$1"
  [[ "${value}" == /* && "${value}" != */ ]]
}

validate_host_port() {
  local value="$1"
  [[ "${value}" =~ ^[^[:space:]:]+:[0-9]+$ ]] || return 1
  validate_port "${value##*:}"
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

validate_bootstrap_admin_password() {
  local value
  value="$(trim "${1-}")"
  [[ ${#value} -ge 12 ]]
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
