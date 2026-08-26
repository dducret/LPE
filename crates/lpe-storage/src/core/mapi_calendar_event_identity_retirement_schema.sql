WITH retirement_table AS (
    SELECT table_row.oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'mapi_calendar_event_identity_retirements'
      AND table_row.relkind = 'r'
),
claims_table AS (
    SELECT table_row.oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'mapi_object_identity_claims'
      AND table_row.relkind = 'r'
)
SELECT
    EXISTS (SELECT 1 FROM retirement_table)
    AND (
        SELECT COUNT(*) = 9
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mapi_calendar_event_identity_retirements'
          AND (
                (column_name IN ('tenant_id', 'account_id', 'event_id')
                    AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name IN ('old_mapi_object_id', 'replacement_mapi_object_id', 'retired_change_number')
                    AND data_type = 'bigint' AND is_nullable = 'NO')
                OR (column_name IN ('old_source_key', 'replacement_source_key')
                    AND data_type = 'bytea' AND is_nullable = 'NO')
                OR (column_name = 'created_at'
                    AND data_type = 'timestamp with time zone'
                    AND is_nullable = 'NO' AND column_default IS NOT NULL)
          )
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'p'
          AND pg_get_constraintdef(constraint_row.oid)
              = 'PRIMARY KEY (tenant_id, account_id, old_mapi_object_id)'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'u'
          AND pg_get_constraintdef(constraint_row.oid) = 'UNIQUE (old_mapi_object_id)'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'u'
          AND pg_get_constraintdef(constraint_row.oid) = 'UNIQUE (old_source_key)'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%old_mapi_object_id%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%65535%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%= 1%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%replacement_mapi_object_id%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%65535%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%= 1%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%octet_length(old_source_key) = 22%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%octet_length(replacement_source_key) = 22%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%retired_change_number >= 43%'
          AND replace(pg_get_constraintdef(constraint_row.oid), '''', '')
              LIKE '%retired_change_number < 140737454800896%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%old_mapi_object_id <> replacement_mapi_object_id%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%old_source_key <> replacement_source_key%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM retirement_table)
          AND constraint_row.contype = 'f'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%FOREIGN KEY (tenant_id, account_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%REFERENCES accounts(tenant_id, id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%ON DELETE CASCADE%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_index index_row
        JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
        JOIN pg_am access_method ON access_method.oid = index_class.relam
        WHERE index_row.indrelid = (SELECT oid FROM retirement_table)
          AND namespace_row.nspname = $1
          AND index_class.relname = 'mapi_calendar_event_identity_retirements_event_idx'
          AND access_method.amname = 'btree'
          AND index_row.indisvalid
          AND index_row.indisready
          AND index_row.indislive
          AND index_row.indexprs IS NULL
          AND index_row.indpred IS NULL
          AND index_row.indnkeyatts = 4
          AND index_row.indnatts = 4
          AND pg_get_indexdef(index_row.indexrelid, 1, FALSE) = 'tenant_id'
          AND pg_get_indexdef(index_row.indexrelid, 2, FALSE) = 'account_id'
          AND pg_get_indexdef(index_row.indexrelid, 3, FALSE) = 'event_id'
          AND pg_get_indexdef(index_row.indexrelid, 4, FALSE) = 'created_at'
    )
    AND EXISTS (SELECT 1 FROM claims_table)
    AND (
        SELECT COUNT(*) = 3
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mapi_object_identity_claims'
          AND (
                (column_name = 'mapi_object_id'
                    AND data_type = 'bigint' AND is_nullable = 'NO')
                OR (column_name = 'source_key'
                    AND data_type = 'bytea' AND is_nullable = 'NO')
                OR (column_name = 'claimed_at'
                    AND data_type = 'timestamp with time zone'
                    AND is_nullable = 'NO' AND column_default IS NOT NULL)
          )
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM claims_table)
          AND constraint_row.contype = 'p'
          AND pg_get_constraintdef(constraint_row.oid) = 'PRIMARY KEY (mapi_object_id)'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM claims_table)
          AND constraint_row.contype = 'u'
          AND pg_get_constraintdef(constraint_row.oid) = 'UNIQUE (source_key)'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM claims_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%mapi_object_id%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%65535%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%= 1%'
    )
    AND EXISTS (
        SELECT 1 FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM claims_table)
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%octet_length(source_key) = 22%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_trigger trigger_row
        JOIN pg_proc function_row ON function_row.oid = trigger_row.tgfoid
        JOIN pg_namespace namespace_row ON namespace_row.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = to_regclass(format('%I.mapi_object_identities', $1))
          AND trigger_row.tgname = 'mapi_object_identities_claim_identity'
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgenabled = 'O'
          AND function_row.proname = 'claim_mapi_object_identity'
          AND namespace_row.nspname = $1
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%AFTER INSERT OR UPDATE OF mapi_object_id, source_key%'
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%FOR EACH ROW EXECUTE FUNCTION%claim_mapi_object_identity()%'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%INSERT INTO mapi_object_identity_claims (mapi_object_id, source_key)%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_trigger trigger_row
        JOIN pg_proc function_row ON function_row.oid = trigger_row.tgfoid
        JOIN pg_namespace namespace_row ON namespace_row.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = to_regclass(format('%I.mapi_special_folder_aliases', $1))
          AND trigger_row.tgname = 'mapi_special_folder_aliases_claim_identity'
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgenabled = 'O'
          AND function_row.proname = 'claim_mapi_special_folder_alias_identity'
          AND namespace_row.nspname = $1
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%AFTER INSERT OR UPDATE OF alias_folder_id, source_key%'
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%FOR EACH ROW EXECUTE FUNCTION%claim_mapi_special_folder_alias_identity()%'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%INSERT INTO mapi_object_identity_claims (mapi_object_id, source_key)%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_trigger trigger_row
        JOIN pg_proc function_row ON function_row.oid = trigger_row.tgfoid
        JOIN pg_namespace namespace_row ON namespace_row.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = (SELECT oid FROM claims_table)
          AND trigger_row.tgname = 'mapi_object_identity_claims_immutable'
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgenabled = 'O'
          AND function_row.proname = 'prevent_mapi_object_identity_claim_mutation'
          AND namespace_row.nspname = $1
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%BEFORE DELETE OR UPDATE%'
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%FOR EACH ROW EXECUTE FUNCTION%prevent_mapi_object_identity_claim_mutation()%'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%MAPI object identity claims are immutable%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_trigger trigger_row
        JOIN pg_proc function_row ON function_row.oid = trigger_row.tgfoid
        JOIN pg_namespace namespace_row ON namespace_row.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = (SELECT oid FROM claims_table)
          AND trigger_row.tgname = 'mapi_object_identity_claims_no_truncate'
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgenabled = 'O'
          AND function_row.proname = 'prevent_mapi_object_identity_claim_mutation'
          AND namespace_row.nspname = $1
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%BEFORE TRUNCATE%'
          AND pg_get_triggerdef(trigger_row.oid) LIKE '%FOR EACH STATEMENT EXECUTE FUNCTION%prevent_mapi_object_identity_claim_mutation()%'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM mapi_object_identities identity
        LEFT JOIN mapi_object_identity_claims claim
          ON claim.mapi_object_id = identity.mapi_object_id
         AND claim.source_key = identity.source_key
        WHERE claim.mapi_object_id IS NULL
    )
    AND NOT EXISTS (
        SELECT 1
        FROM mapi_special_folder_aliases alias
        LEFT JOIN mapi_object_identity_claims claim
          ON claim.mapi_object_id = alias.alias_folder_id
         AND claim.source_key = alias.source_key
        WHERE claim.mapi_object_id IS NULL
    )
    AND NOT EXISTS (
        SELECT 1
        FROM mapi_calendar_event_identity_retirements retirement
        LEFT JOIN mapi_object_identity_claims old_claim
          ON old_claim.mapi_object_id = retirement.old_mapi_object_id
         AND old_claim.source_key = retirement.old_source_key
        LEFT JOIN mapi_object_identity_claims replacement_claim
          ON replacement_claim.mapi_object_id = retirement.replacement_mapi_object_id
         AND replacement_claim.source_key = retirement.replacement_source_key
        WHERE old_claim.mapi_object_id IS NULL
           OR replacement_claim.mapi_object_id IS NULL
    );
