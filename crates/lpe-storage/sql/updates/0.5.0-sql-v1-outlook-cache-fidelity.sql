BEGIN;

SET LOCAL search_path = pg_catalog, public;

DO $version_guard$
DECLARE
    installed_schema_version TEXT;
BEGIN
    IF to_regclass('public.schema_metadata') IS NULL THEN
        RAISE EXCEPTION 'LPE schema metadata is missing; this update only supports 0.5.0-sql-v1';
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
END
$version_guard$;

CREATE TABLE IF NOT EXISTS public.mapi_local_replica_id_ranges (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    replica_guid UUID NOT NULL,
    first_global_counter BIGINT NOT NULL CHECK (first_global_counter >= 43 AND first_global_counter < 140737454800896),
    end_global_counter_exclusive BIGINT NOT NULL CHECK (end_global_counter_exclusive > 43 AND end_global_counter_exclusive <= 140737454800896),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, replica_guid, first_global_counter),
    CHECK (first_global_counter < end_global_counter_exclusive),
    FOREIGN KEY (tenant_id, account_id, replica_guid)
        REFERENCES public.mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mapi_local_replica_id_ranges_membership_idx
    ON public.mapi_local_replica_id_ranges (
        tenant_id,
        account_id,
        replica_guid,
        first_global_counter,
        end_global_counter_exclusive
    );

CREATE TABLE IF NOT EXISTS public.mapi_local_replica_deleted_ranges (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    folder_id BIGINT NOT NULL CHECK (folder_id > 0),
    replica_guid UUID NOT NULL,
    min_global_counter BIGINT NOT NULL CHECK (min_global_counter >= 43 AND min_global_counter < 140737454800896),
    max_global_counter BIGINT NOT NULL CHECK (max_global_counter >= 43 AND max_global_counter < 140737454800896),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, folder_id, replica_guid, min_global_counter, max_global_counter),
    CHECK (min_global_counter <= max_global_counter),
    FOREIGN KEY (tenant_id, account_id, replica_guid)
        REFERENCES public.mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mapi_local_replica_deleted_ranges_folder_idx
    ON public.mapi_local_replica_deleted_ranges (
        tenant_id,
        account_id,
        folder_id,
        replica_guid,
        min_global_counter,
        max_global_counter
    );

DO $wlink_ordinal$
DECLARE
    ordinal_data_type TEXT;
BEGIN
    IF to_regclass('public.mapi_navigation_shortcuts') IS NULL THEN
        RAISE EXCEPTION
            'MAPI navigation shortcut table is missing; initialize a fresh LPE 0.5.0 database';
    END IF;

    SELECT data_type
    INTO ordinal_data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mapi_navigation_shortcuts'
      AND column_name = 'ordinal';

    IF ordinal_data_type = 'bigint' THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ALTER COLUMN ordinal DROP DEFAULT;
        ALTER TABLE public.mapi_navigation_shortcuts
            DROP CONSTRAINT IF EXISTS mapi_navigation_shortcuts_ordinal_check;
        ALTER TABLE public.mapi_navigation_shortcuts
            ALTER COLUMN ordinal TYPE BYTEA
            -- [MS-OXOCFG] section 2.2.9.7 reserves 0x00 and 0xFF as
            -- final bytes. Preserve valid compact projections; reserved
            -- endings use an injective four-byte big-endian value plus 0x80.
            USING CASE
                WHEN (ordinal & 255) IN (0, 255) THEN
                    decode(lpad(to_hex(ordinal), 8, '0') || '80', 'hex')
                ELSE
                    decode(
                        lpad(
                            to_hex(ordinal),
                            ((length(to_hex(ordinal)) + 1) / 2) * 2,
                            '0'
                        ),
                        'hex'
                    )
            END;
    ELSIF ordinal_data_type = 'bytea' THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ALTER COLUMN ordinal DROP DEFAULT;
    ELSE
        RAISE EXCEPTION
            'MAPI navigation shortcut ordinal has unsupported type %; expected bigint or bytea',
            COALESCE(ordinal_data_type, '<missing>');
    END IF;
END
$wlink_ordinal$;

ALTER TABLE public.mapi_navigation_shortcuts
    ADD COLUMN IF NOT EXISTS calendar_color INTEGER,
    ADD COLUMN IF NOT EXISTS address_book_entry_id BYTEA,
    ADD COLUMN IF NOT EXISTS address_book_store_entry_id BYTEA,
    ADD COLUMN IF NOT EXISTS client_id BYTEA,
    ADD COLUMN IF NOT EXISTS ro_group_type INTEGER;

DO $wlink_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_ordinal_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_ordinal_check CHECK (
                octet_length(ordinal) > 0
                AND octet_length(ordinal) <= 65535
                AND get_byte(ordinal, octet_length(ordinal) - 1) <> 0
                AND get_byte(ordinal, octet_length(ordinal) - 1) <> 255
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_calendar_color_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_calendar_color_check
            CHECK (calendar_color >= -1 AND calendar_color <= 14);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_address_book_entry_id_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_address_book_entry_id_check CHECK (
                octet_length(address_book_entry_id) > 0
                AND octet_length(address_book_entry_id) <= 65535
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_address_book_store_entry_id_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_address_book_store_entry_id_check CHECK (
                octet_length(address_book_store_entry_id) > 0
                AND octet_length(address_book_store_entry_id) <= 65535
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_client_id_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_client_id_check CHECK (
                octet_length(client_id) > 0
                AND octet_length(client_id) <= 65535
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_ro_group_type_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_ro_group_type_check
            CHECK (ro_group_type >= -1 AND ro_group_type <= 4);
    END IF;
END
$wlink_constraints$;

DO $associated_config_index$
DECLARE
    logical_index_definition TEXT;
    logical_index_is_unique BOOLEAN;
BEGIN
    IF to_regclass('public.mapi_associated_config_messages') IS NULL THEN
        RAISE EXCEPTION
            'MAPI associated configuration table is missing; initialize a fresh LPE 0.5.0 database';
    END IF;

    SELECT replace(pg_get_indexdef(index_row.indexrelid), 'public.', ''), index_row.indisunique
    INTO logical_index_definition, logical_index_is_unique
    FROM pg_index index_row
    JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
    JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
    WHERE namespace_row.nspname = 'public'
      AND index_class.relname = 'mapi_associated_config_messages_logical_idx';

    IF logical_index_definition IS NULL THEN
        CREATE INDEX mapi_associated_config_messages_logical_idx
            ON public.mapi_associated_config_messages (
                tenant_id, account_id, folder_id, message_class, subject
            );
    ELSIF logical_index_definition LIKE
            'CREATE UNIQUE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)%'
          AND logical_index_is_unique THEN
        DROP INDEX public.mapi_associated_config_messages_logical_idx;
        CREATE INDEX mapi_associated_config_messages_logical_idx
            ON public.mapi_associated_config_messages (
                tenant_id, account_id, folder_id, message_class, subject
            );
    ELSIF logical_index_definition NOT LIKE
            'CREATE INDEX mapi_associated_config_messages_logical_idx ON mapi_associated_config_messages USING btree (tenant_id, account_id, folder_id, message_class, subject)%'
          OR logical_index_is_unique THEN
        RAISE EXCEPTION
            'MAPI associated configuration logical index has an incompatible definition: %',
            logical_index_definition;
    END IF;
END
$associated_config_index$;

DO $validation$
DECLARE
    local_replica_range_shape_ok BOOLEAN;
    wlink_fidelity_shape_ok BOOLEAN;
BEGIN
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
    SELECT
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
    INTO local_replica_range_shape_ok;

    IF NOT local_replica_range_shape_ok THEN
        RAISE EXCEPTION
            'MAPI local replica range table shape is incomplete; initialize a fresh LPE 0.5.0 database';
    END IF;

    SELECT
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
            WHERE constraint_row.conrelid = 'public.mapi_navigation_shortcuts'::regclass
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
    INTO wlink_fidelity_shape_ok;

    IF NOT wlink_fidelity_shape_ok THEN
        RAISE EXCEPTION
            'MAPI WLink/configuration FAI fidelity shape is incomplete; initialize a fresh LPE 0.5.0 database';
    END IF;
END
$validation$;

COMMIT;
