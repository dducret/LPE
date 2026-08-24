WITH message_table AS (
    SELECT table_row.oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'messages'
      AND table_row.relkind = 'r'
), classification_table AS (
    SELECT table_row.oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'calendar_mail_classifications'
      AND table_row.relkind = 'r'
), projection_table AS (
    SELECT table_row.oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'calendar_mail_classification_projections'
      AND table_row.relkind = 'r'
)
SELECT
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'messages'
          AND column_name = 'authorized_calendar_response_content_sha256'
          AND data_type = 'text'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'messages'
          AND column_name = 'calendar_response_processed'
          AND data_type = 'boolean'
          AND is_nullable = 'NO'
          AND column_default = 'false'
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mailbox_messages'
          AND column_name = 'calendar_request_processed'
          AND data_type = 'boolean'
          AND is_nullable = 'NO'
          AND column_default = 'false'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM message_table)
          AND constraint_row.conname = 'messages_authorized_calendar_response_content_sha256_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%authorized_calendar_response_content_sha256 IS NULL%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%^[0-9a-f]{64}$%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM message_table)
          AND constraint_row.conname = 'messages_calendar_response_processed_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%NOT calendar_response_processed%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%authorized_calendar_response_content_sha256 IS NOT NULL%'
    )
    AND (
        SELECT COUNT(*) = 11
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'calendar_mail_classifications'
    )
    AND (
        SELECT COUNT(*) = 11
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'calendar_mail_classifications'
          AND (
                (column_name = 'tenant_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name = 'message_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name = 'parser_revision' AND data_type = 'integer' AND is_nullable = 'NO' AND column_default IS NULL)
                OR (column_name = 'classification_generation' AND data_type = 'bigint' AND is_nullable = 'NO' AND column_default = '1')
                OR (column_name = 'requires_projection_rotation' AND data_type = 'boolean' AND is_nullable = 'NO' AND column_default = 'false')
                OR (column_name = 'needs_reclassification' AND data_type = 'boolean' AND is_nullable = 'NO' AND column_default = 'false')
                OR (column_name = 'classification' AND data_type = 'text' AND is_nullable = 'NO' AND column_default IS NULL)
                OR (column_name = 'scheduling_mime_part_id' AND data_type = 'uuid' AND is_nullable = 'YES' AND column_default IS NULL)
                OR (column_name = 'metadata_json' AND data_type = 'jsonb' AND is_nullable = 'NO' AND column_default IS NULL)
                OR (column_name = 'created_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO' AND column_default = 'now()')
                OR (column_name = 'updated_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO' AND column_default = 'now()')
          )
    )
    AND (
        SELECT COUNT(*) = 6
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'calendar_mail_classification_projections'
    )
    AND (
        SELECT COUNT(*) = 6
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'calendar_mail_classification_projections'
          AND (
                (column_name = 'tenant_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name = 'account_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name = 'message_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                OR (column_name = 'applied_generation' AND data_type = 'bigint' AND is_nullable = 'NO' AND column_default IS NULL)
                OR (column_name = 'created_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO' AND column_default = 'now()')
                OR (column_name = 'updated_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO' AND column_default = 'now()')
          )
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mime_parts'
          AND column_name = 'is_scheduling_body'
          AND data_type = 'boolean'
          AND is_nullable = 'NO'
          AND column_default = 'false'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
        WHERE namespace_row.nspname = $1
          AND table_row.relname = 'mime_parts'
          AND constraint_row.conname = 'mime_parts_scheduling_body_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%is_scheduling_body%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%lower(btrim(split_part(content_type, '';''::text, 1))) = ''text/calendar''::text%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%content_disposition IS DISTINCT FROM%attachment%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%blob_id IS NOT NULL%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_index index_row
        JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
        JOIN pg_class table_row ON table_row.oid = index_row.indrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
        WHERE namespace_row.nspname = $1
          AND table_row.relname = 'mime_parts'
          AND index_class.relname = 'mime_parts_one_scheduling_body_idx'
          AND index_row.indisunique
          AND index_row.indisvalid
          AND index_row.indisready
          AND index_row.indislive
          AND pg_get_indexdef(index_row.indexrelid) LIKE '%USING btree (tenant_id, message_id)%'
          AND pg_get_expr(index_row.indpred, index_row.indrelid) = 'is_scheduling_body'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.contype = 'p'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) = 'PRIMARY KEY (tenant_id, message_id)'
    )
    AND (
        SELECT COUNT(*) = 5
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.conname IN (
                'calendar_mail_classifications_parser_revision_check',
                'calendar_mail_classifications_generation_check',
                'calendar_mail_classifications_classification_check',
                'calendar_mail_classifications_metadata_object_check',
                'calendar_mail_classifications_metadata_shape_check'
          )
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_parser_revision_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%parser_revision > 0%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_generation_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%classification_generation > 0%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_classification_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%none%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%request%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%response%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_metadata_object_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%jsonb_typeof(metadata_json) =%object%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_metadata_shape_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%needs_reclassification%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%scheduling_mime_part_id IS NULL%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%scheduling_mime_part_id IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%metadata_json%kind%none%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%metadata_json%request%IS TRUE%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%metadata_json%response%IS TRUE%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%jsonb_typeof%request%object%IS TRUE%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%jsonb_typeof%response%object%IS TRUE%'
    )
    AND (
        SELECT COUNT(*) = 2
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.contype = 'f'
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.conname IN (
                'calendar_mail_classifications_message_fkey',
                'calendar_mail_classifications_mime_part_fkey'
          )
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_message_fkey'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%FOREIGN KEY (tenant_id, message_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%REFERENCES messages(tenant_id, id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%ON DELETE CASCADE%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM classification_table)
          AND constraint_row.conname = 'calendar_mail_classifications_mime_part_fkey'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%FOREIGN KEY (tenant_id, message_id, scheduling_mime_part_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%REFERENCES mime_parts(tenant_id, message_id, id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%ON DELETE CASCADE%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
          AND constraint_row.contype = 'p'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) = 'PRIMARY KEY (tenant_id, account_id, message_id)'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
          AND constraint_row.conname = 'calendar_mail_classification_projections_generation_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%applied_generation > 0%'
    )
    AND (
        SELECT COUNT(*) = 2
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
          AND constraint_row.contype = 'f'
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.conname IN (
                'calendar_mail_classification_projections_account_fkey',
                'calendar_mail_classification_projections_classification_fkey'
          )
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
          AND constraint_row.conname = 'calendar_mail_classification_projections_account_fkey'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%FOREIGN KEY (tenant_id, account_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%REFERENCES accounts(tenant_id, id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%ON DELETE CASCADE%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
          AND constraint_row.conname = 'calendar_mail_classification_projections_classification_fkey'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%FOREIGN KEY (tenant_id, message_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%REFERENCES calendar_mail_classifications(tenant_id, message_id)%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%ON DELETE CASCADE%'
    )
