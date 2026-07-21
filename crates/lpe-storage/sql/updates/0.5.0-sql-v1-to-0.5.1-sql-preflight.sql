BEGIN;

SET TRANSACTION READ ONLY;
SET LOCAL search_path = pg_catalog, public;

DO $source_shape_preflight$
DECLARE
    installed_schema_version TEXT;
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

    SELECT COUNT(*) = 6
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name IN (
          'mail_change_log',
          'calendar_events',
          'mapi_mailbox_replicas',
          'mapi_object_identities',
          'mapi_calendar_event_identity_moves',
          'mapi_special_folder_aliases'
      )
    INTO shape_ok;

    IF NOT shape_ok THEN
        RAISE EXCEPTION
            'unsupported 0.5.0-sql-v1 physical shape: required Calendar/MAPI tables are missing; initialize a fresh LPE 0.5.1 database';
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
              AND pg_get_constraintdef(oid) LIKE '%octet_length(change_key) >= 17%'
              AND pg_get_constraintdef(oid) LIKE '%octet_length(change_key) <= 24%'
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
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'u'
              AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, source_key)'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'u'
              AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, account_id, mapi_change_number)'
        )
        AND EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass('public.mapi_special_folder_aliases')
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number >=%43%'
              AND pg_get_constraintdef(oid) LIKE '%mapi_change_number <%140737454800896%'
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
