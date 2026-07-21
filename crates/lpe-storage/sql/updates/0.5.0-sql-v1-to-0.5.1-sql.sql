BEGIN;

SET LOCAL search_path = pg_catalog, public;

DO $schema_version_transition$
DECLARE
    installed_schema_version TEXT;
    target_shape_ok BOOLEAN;
BEGIN
    IF to_regclass('public.schema_metadata') IS NULL THEN
        RAISE EXCEPTION
            'LPE schema metadata is missing; this update only supports 0.5.0-sql-v1 and 0.5.1-sql';
    END IF;

    SELECT schema_version
    INTO installed_schema_version
    FROM public.schema_metadata
    WHERE singleton = TRUE;

    IF installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'
       AND installed_schema_version IS DISTINCT FROM '0.5.1-sql' THEN
        RAISE EXCEPTION
            'unsupported LPE schema version: expected 0.5.0-sql-v1 or 0.5.1-sql, found %',
            COALESCE(installed_schema_version, '<missing>');
    END IF;

    IF current_setting('lpe.schema_target_shape_validated', TRUE)
       IS DISTINCT FROM '0.5.1-sql' THEN
        RAISE EXCEPTION
            'the 0.5.1 label transition requires a validated update-lpe.sh session';
    END IF;

    SELECT
        (
            SELECT COUNT(*) = 8
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name IN (
                  'calendar_events',
                  'mapi_object_identities',
                  'mapi_calendar_event_identity_moves',
                  'mapi_special_folder_aliases',
                  'mapi_navigation_shortcuts',
                  'mapi_associated_config_messages',
                  'mapi_local_replica_id_ranges',
                  'mapi_local_replica_deleted_ranges'
              )
        )
        AND (
            SELECT COUNT(*) = 2
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_object_identities'
              AND (
                  (column_name = 'mapi_change_number' AND data_type = 'bigint' AND is_nullable = 'NO')
                  OR (column_name = 'predecessor_change_list' AND data_type = 'bytea' AND is_nullable = 'NO')
              )
        )
        AND (
            SELECT COUNT(*) = 2
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'calendar_events'
              AND (
                  (column_name = 'lifecycle_state' AND data_type = 'text' AND is_nullable = 'NO')
                  OR (column_name = 'deleted_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'YES')
              )
        )
        AND (
            SELECT COUNT(*) = 6
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_navigation_shortcuts'
              AND (
                  (column_name = 'ordinal' AND data_type = 'bytea' AND is_nullable = 'NO')
                  OR (column_name IN ('address_book_entry_id', 'address_book_store_entry_id', 'client_id') AND data_type = 'bytea' AND is_nullable = 'YES')
                  OR (column_name IN ('calendar_color', 'ro_group_type') AND data_type = 'integer' AND is_nullable = 'YES')
              )
        )
        AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'mapi_special_folder_aliases'
              AND column_name = 'mapi_change_number'
              AND data_type = 'bigint'
              AND is_nullable = 'NO'
        )
        AND (
            SELECT COUNT(DISTINCT conrelid) = 2
            FROM pg_constraint
            WHERE conrelid IN (
                to_regclass('public.mail_change_log'),
                to_regclass('public.mapi_object_identities')
            )
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
        AND (
            SELECT COUNT(*) = 2
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_calendar_event_identity_moves')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(%change_key) >= 17%'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(%change_key) <= 24%'
        )
        AND (
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
        AND (
            SELECT COUNT(*) = 2
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname IN (
                  'mapi_local_replica_id_ranges_membership_idx',
                  'mapi_local_replica_deleted_ranges_folder_idx'
              )
        )
        AND EXISTS (
            SELECT 1
            FROM pg_index index_row
            JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
            WHERE namespace_row.nspname = 'public'
              AND index_class.relname = 'mapi_associated_config_messages_logical_idx'
              AND NOT index_row.indisunique
              AND index_row.indisvalid
              AND index_row.indisready
              AND index_row.indislive
              AND replace(pg_get_indexdef(index_row.indexrelid), 'public.', '') =
                  'CREATE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)'
        )
    INTO target_shape_ok;

    IF NOT COALESCE(target_shape_ok, FALSE) THEN
        RAISE EXCEPTION
            'LPE 0.5.1 target physical shape is incomplete; run update-lpe.sh or initialize a fresh database';
    END IF;

    ALTER TABLE public.schema_metadata
        DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check;

    UPDATE public.schema_metadata
    SET schema_version = '0.5.1-sql'
    WHERE singleton = TRUE;

    ALTER TABLE public.schema_metadata
        ADD CONSTRAINT schema_metadata_schema_version_check
        CHECK (schema_version = '0.5.1-sql');
END
$schema_version_transition$;

RESET lpe.schema_target_shape_validated;

COMMIT;
