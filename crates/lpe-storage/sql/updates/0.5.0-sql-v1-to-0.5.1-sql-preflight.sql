BEGIN;

SET TRANSACTION READ ONLY;
SET LOCAL search_path = pg_catalog, public;

DO $source_shape_preflight$
DECLARE
    installed_schema_version TEXT;
    local_replica_table_count INTEGER;
    ordinal_data_type TEXT;
    shape_ok BOOLEAN;
BEGIN
    IF to_regclass('public.schema_metadata') IS NULL THEN
        RAISE EXCEPTION
            'LPE schema metadata is missing; this preflight only supports 0.5.0-sql-v1';
    END IF;

    SELECT schema_version
    INTO installed_schema_version
    FROM public.schema_metadata
    WHERE singleton = TRUE;

    IF installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1' THEN
        RAISE EXCEPTION
            'unsupported LPE schema version: expected 0.5.0-sql-v1, found %',
            COALESCE(installed_schema_version, '<missing>');
    END IF;

    SELECT COUNT(*) = 8
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name IN (
          'mail_change_log',
          'calendar_events',
          'mapi_mailbox_replicas',
          'mapi_object_identities',
          'mapi_calendar_event_identity_moves',
          'mapi_special_folder_aliases',
          'mapi_navigation_shortcuts',
          'mapi_associated_config_messages'
      )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: required Calendar/MAPI tables are missing; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT data_type
    INTO ordinal_data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_navigation_shortcuts'
      AND column_name = 'ordinal'
      AND is_nullable = 'NO';

    SELECT COUNT(*) = 5
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_associated_config_messages'
      AND is_nullable = 'NO'
      AND (
          (column_name IN ('tenant_id', 'account_id') AND data_type = 'uuid')
          OR (column_name = 'folder_id' AND data_type = 'bigint')
          OR (column_name IN ('message_class', 'subject') AND data_type = 'text')
      )
    INTO shape_ok;

    IF ordinal_data_type IS NULL
       OR ordinal_data_type NOT IN ('bigint', 'bytea')
       OR NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: Outlook cache-fidelity source columns are missing or incompatible; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_navigation_shortcuts'
              AND column_name IN (
                  'calendar_color',
                  'address_book_entry_id',
                  'address_book_store_entry_id',
                  'client_id',
                  'ro_group_type'
              )
              AND NOT (
                  (column_name IN ('calendar_color', 'ro_group_type')
                   AND data_type = 'integer'
                   AND is_nullable = 'YES')
                  OR (column_name IN (
                          'address_book_entry_id',
                          'address_book_store_entry_id',
                          'client_id'
                      )
                      AND data_type = 'bytea'
                      AND is_nullable = 'YES')
              )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = to_regclass('public.mapi_navigation_shortcuts')
              AND constraint_row.conname IN (
                  'mapi_navigation_shortcuts_ordinal_check',
                  'mapi_navigation_shortcuts_calendar_color_check',
                  'mapi_navigation_shortcuts_address_book_entry_id_check',
                  'mapi_navigation_shortcuts_address_book_store_entry_id_check',
                  'mapi_navigation_shortcuts_client_id_check',
                  'mapi_navigation_shortcuts_ro_group_type_check'
              )
              AND NOT (
                  (constraint_row.conname = 'mapi_navigation_shortcuts_ordinal_check'
                   AND ordinal_data_type = 'bigint')
                  OR (constraint_row.conname = 'mapi_navigation_shortcuts_ordinal_check'
                      AND ordinal_data_type = 'bytea'
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
        AND NOT EXISTS (
            SELECT 1
            FROM pg_index index_row
            JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
            WHERE namespace_row.nspname = 'public'
              AND index_class.relname = 'mapi_associated_config_messages_logical_idx'
              AND NOT (
                  (index_row.indisunique
                   AND replace(pg_get_indexdef(index_row.indexrelid), 'public.', '') LIKE
                       'CREATE UNIQUE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)%')
                  OR (NOT index_row.indisunique
                      AND replace(pg_get_indexdef(index_row.indexrelid), 'public.', '') LIKE
                          'CREATE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)%')
              )
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: Outlook cache-fidelity objects are incompatible; initialize a fresh LPE 0.5.1 database';
    END IF;

    IF ordinal_data_type = 'bigint' THEN
        SELECT NOT EXISTS (
            SELECT 1
            FROM public.mapi_navigation_shortcuts
            WHERE ordinal < 0 OR ordinal > 4294967295
        )
        INTO shape_ok;

        IF NOT shape_ok THEN
            RAISE EXCEPTION
                'unsupported 0.5.0-sql-v1 physical shape: existing numeric WLink ordinals are outside the supported range; initialize a fresh LPE 0.5.1 database';
        END IF;
    ELSE
        SELECT NOT EXISTS (
            SELECT 1
            FROM public.mapi_navigation_shortcuts
            WHERE octet_length(ordinal) = 0
               OR octet_length(ordinal) > 65535
               OR CASE
                      WHEN octet_length(ordinal) > 0
                      THEN get_byte(ordinal, octet_length(ordinal) - 1) IN (0, 255)
                      ELSE FALSE
                  END
        )
        INTO shape_ok;

        IF NOT shape_ok THEN
            RAISE EXCEPTION
                'unsupported 0.5.0-sql-v1 physical shape: existing binary WLink ordinals cannot receive the cache-fidelity constraints; initialize a fresh LPE 0.5.1 database';
        END IF;
    END IF;

    SELECT COUNT(*)
    INTO local_replica_table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name IN (
          'mapi_local_replica_id_ranges',
          'mapi_local_replica_deleted_ranges'
      );

    IF local_replica_table_count NOT IN (0, 2) THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: local-replica range tables are only partially present; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        COUNT(*) = 2
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%deleted_calendar_event%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND convalidated
              AND conname = 'mapi_object_identities_source_key_check'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(source_key) = 22%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND convalidated
              AND conname = 'mapi_object_identities_change_key_check'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(change_key) >= 17%'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(change_key) <= 24%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND convalidated
              AND conname = 'mapi_object_identities_instance_key_check'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(instance_key) = 22%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number > 0%'
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number <=%140737488355327%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_object_identities')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(predecessor_change_list) > 0%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_index index_row
            JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
            JOIN pg_am access_method ON access_method.oid = index_class.relam
            WHERE index_row.indrelid = to_regclass('public.mapi_object_identities')
              AND namespace_row.nspname = 'public'
              AND index_class.relname = 'mapi_object_identities_active_source_key_uidx'
              AND access_method.amname = 'btree'
              AND index_row.indisunique
              AND index_row.indisvalid
              AND index_row.indisready
              AND index_row.indislive
              AND index_row.indexprs IS NULL
              AND index_row.indnkeyatts = 3
              AND index_row.indnatts = 3
              AND pg_get_indexdef(index_row.indexrelid, 1, FALSE) = 'tenant_id'
              AND pg_get_indexdef(index_row.indexrelid, 2, FALSE) = 'account_id'
              AND pg_get_indexdef(index_row.indexrelid, 3, FALSE) = 'source_key'
              AND lower(regexp_replace(
                    pg_get_expr(index_row.indpred, index_row.indrelid, FALSE),
                    '[()[:space:]]',
                    '',
                    'g'
                  )) = 'deleted_atisnull'
        )
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_object_identities'
      AND (
          (column_name = 'mapi_change_number' AND data_type = 'bigint' AND is_nullable = 'NO')
          OR (column_name = 'predecessor_change_list' AND data_type = 'bytea' AND is_nullable = 'NO')
      )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: durable MAPI identity version columns or constraints are stale; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        (
            SELECT COUNT(*) = 2
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mail_change_log')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%deleted_calendar_event%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'mail_change_log'
              AND indexname = 'mail_change_log_collaboration_idx'
              AND indexdef LIKE '%deleted_calendar_event%'
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: Deleted Calendar change-log constraints or index are stale; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        (
            SELECT COUNT(*) = 2
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'calendar_events'
              AND (
                  (column_name = 'lifecycle_state' AND data_type = 'text' AND is_nullable = 'NO')
                  OR (column_name = 'deleted_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'YES')
              )
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.calendar_events')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%lifecycle_state%active%deleted_at IS NULL%'
              AND pg_get_constraintdef(oid) LIKE '%lifecycle_state%deleted%deleted_at IS NOT NULL%'
        )
        AND (
            SELECT COUNT(*) = 3
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'calendar_events'
              AND (
                  (indexname = 'calendar_events_owner_time_idx' AND indexdef LIKE '%lifecycle_state%active%')
                  OR (indexname = 'calendar_events_owner_reminder_idx' AND indexdef LIKE '%lifecycle_state%active%' AND indexdef LIKE '%reminder_set%')
                  OR (indexname = 'calendar_events_owner_deleted_idx' AND indexdef LIKE '%lifecycle_state%deleted%')
              )
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: Calendar Deleted Items lifecycle columns, constraint, or indexes are stale; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        (
            SELECT COUNT(*) = 15
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_calendar_event_identity_moves'
              AND is_nullable = 'NO'
              AND (
                  (data_type = 'uuid' AND column_name IN ('tenant_id', 'account_id', 'event_id'))
                  OR (data_type = 'bigint' AND column_name IN ('old_mapi_object_id', 'new_mapi_object_id', 'old_change_number', 'new_change_number'))
                  OR (data_type = 'bytea' AND column_name IN ('old_source_key', 'new_source_key', 'old_change_key', 'new_change_key', 'old_instance_key', 'new_instance_key', 'new_predecessor_change_list'))
                  OR (data_type = 'timestamp with time zone' AND column_name = 'created_at')
              )
        )
        AND (
            SELECT COUNT(*) = 2
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_calendar_event_identity_moves')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(%change_key) >= 17%'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(%change_key) <= 24%'
        )
        AND (
            SELECT COUNT(*) = 2
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'mapi_calendar_event_identity_moves'
              AND indexname IN (
                  'mapi_calendar_event_identity_moves_old_id_idx',
                  'mapi_calendar_event_identity_moves_old_source_key_idx'
              )
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: Calendar identity-move storage is stale; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_mailbox_replicas'
              AND column_name = 'next_global_counter'
              AND data_type = 'bigint'
              AND is_nullable = 'NO'
              AND column_default LIKE '43%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_mailbox_replicas')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%next_global_counter >=%43%'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM public.mapi_mailbox_replicas
            WHERE next_global_counter < 43
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: MAPI replica counters predate the reserved-ID range; initialize a fresh LPE 0.5.1 database';
    END IF;

    SELECT
        (
            SELECT COUNT(*) = 6
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_special_folder_aliases'
              AND column_name IN (
                  'tenant_id', 'account_id', 'alias_folder_id',
                  'canonical_folder_id', 'source_key', 'mapi_change_number'
              )
              AND is_nullable = 'NO'
              AND data_type = CASE column_name
                  WHEN 'tenant_id' THEN 'uuid'
                  WHEN 'account_id' THEN 'uuid'
                  WHEN 'alias_folder_id' THEN 'bigint'
                  WHEN 'canonical_folder_id' THEN 'bigint'
                  WHEN 'source_key' THEN 'bytea'
                  WHEN 'mapi_change_number' THEN 'bigint'
              END
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%alias_folder_id >= 2818049%'
              AND replace(pg_get_constraintdef(oid), '''', '') LIKE '%alias_folder_id < 9223369837831520257%'
              AND pg_get_constraintdef(oid) LIKE '%65535%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%canonical_folder_id > 0%'
              AND pg_get_constraintdef(oid) LIKE '%canonical_folder_id <= 2752513%'
              AND pg_get_constraintdef(oid) LIKE '%65535%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%octet_length(source_key) = 22%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number >=%43%'
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number <%140737454800896%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%alias_folder_id <> canonical_folder_id%'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'p'
              AND convalidated
              AND pg_get_constraintdef(oid) = 'PRIMARY KEY (tenant_id, account_id, alias_folder_id)'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'u'
              AND convalidated
              AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, source_key)'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'u'
              AND convalidated
              AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, mapi_change_number)'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'f'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%FOREIGN KEY (tenant_id, account_id)%'
              AND pg_get_constraintdef(oid) LIKE '%REFERENCES accounts(tenant_id, id)%'
              AND pg_get_constraintdef(oid) LIKE '%ON DELETE CASCADE%'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'mapi_special_folder_aliases'
              AND indexdef LIKE 'CREATE UNIQUE INDEX%canonical_folder_id%'
        )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: MAPI special-folder aliases are stale; initialize a fresh LPE 0.5.1 database';
    END IF;
END
$source_shape_preflight$;

COMMIT;
