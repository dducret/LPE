-- Read-only canonical-shape predicate for actionable calendar-mail state.
WITH relation_oids AS (
    SELECT
        (MAX(table_row.oid::bigint) FILTER (
            WHERE table_row.relname = 'messages' AND table_row.relkind = 'r'
        ))::oid AS messages_oid,
        (MAX(table_row.oid::bigint) FILTER (
            WHERE table_row.relname = 'accounts' AND table_row.relkind = 'r'
        ))::oid AS accounts_oid,
        (MAX(table_row.oid::bigint) FILTER (
            WHERE table_row.relname = 'mime_parts' AND table_row.relkind = 'r'
        ))::oid AS mime_parts_oid,
        (MAX(table_row.oid::bigint) FILTER (
            WHERE table_row.relname = 'calendar_mail_classifications'
              AND table_row.relkind = 'r'
        ))::oid AS classifications_oid,
        (MAX(table_row.oid::bigint) FILTER (
            WHERE table_row.relname = 'calendar_mail_classification_projections'
              AND table_row.relkind = 'r'
        ))::oid AS projections_oid
    FROM pg_class table_row
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = 'public'
), expected_columns(table_name, column_name, data_type, is_nullable, default_kind) AS (
    VALUES
        ('messages'::text, 'authorized_calendar_response_content_sha256'::text, 'text'::text, 'YES'::text, 'none'::text),
        ('messages', 'calendar_response_processed', 'boolean', 'NO', 'false'),
        ('mailbox_messages', 'calendar_request_processed', 'boolean', 'NO', 'false'),
        ('mime_parts'::text, 'is_scheduling_body'::text, 'boolean'::text, 'NO'::text, 'false'::text),
        ('calendar_mail_classifications', 'tenant_id', 'uuid', 'NO', 'none'),
        ('calendar_mail_classifications', 'message_id', 'uuid', 'NO', 'none'),
        ('calendar_mail_classifications', 'parser_revision', 'integer', 'NO', 'none'),
        ('calendar_mail_classifications', 'classification_generation', 'bigint', 'NO', 'one'),
        ('calendar_mail_classifications', 'requires_projection_rotation', 'boolean', 'NO', 'false'),
        ('calendar_mail_classifications', 'needs_reclassification', 'boolean', 'NO', 'false'),
        ('calendar_mail_classifications', 'classification', 'text', 'NO', 'none'),
        ('calendar_mail_classifications', 'scheduling_mime_part_id', 'uuid', 'YES', 'none'),
        ('calendar_mail_classifications', 'metadata_json', 'jsonb', 'NO', 'none'),
        ('calendar_mail_classifications', 'created_at', 'timestamp with time zone', 'NO', 'now'),
        ('calendar_mail_classifications', 'updated_at', 'timestamp with time zone', 'NO', 'now'),
        ('calendar_mail_classification_projections', 'tenant_id', 'uuid', 'NO', 'none'),
        ('calendar_mail_classification_projections', 'account_id', 'uuid', 'NO', 'none'),
        ('calendar_mail_classification_projections', 'message_id', 'uuid', 'NO', 'none'),
        ('calendar_mail_classification_projections', 'applied_generation', 'bigint', 'NO', 'none'),
        ('calendar_mail_classification_projections', 'created_at', 'timestamp with time zone', 'NO', 'now'),
        ('calendar_mail_classification_projections', 'updated_at', 'timestamp with time zone', 'NO', 'now')
), matched_columns AS (
    SELECT expected.table_name, expected.column_name
    FROM expected_columns expected
    JOIN pg_class table_row
      ON table_row.relname = expected.table_name
     AND table_row.relkind = 'r'
    JOIN pg_namespace namespace_row
      ON namespace_row.oid = table_row.relnamespace
     AND namespace_row.nspname = 'public'
    JOIN pg_attribute actual
      ON actual.attrelid = table_row.oid
     AND actual.attname = expected.column_name
     AND NOT actual.attisdropped
     AND format_type(actual.atttypid, actual.atttypmod) = expected.data_type
     AND CASE expected.is_nullable
           WHEN 'NO' THEN actual.attnotnull
           WHEN 'YES' THEN NOT actual.attnotnull
         END
    LEFT JOIN pg_attrdef default_row
      ON default_row.adrelid = actual.attrelid
     AND default_row.adnum = actual.attnum
    WHERE CASE expected.default_kind
           WHEN 'none' THEN default_row.oid IS NULL
           WHEN 'false' THEN pg_get_expr(default_row.adbin, default_row.adrelid) = 'false'
           WHEN 'one' THEN pg_get_expr(default_row.adbin, default_row.adrelid) = '1'
           WHEN 'now' THEN pg_get_expr(default_row.adbin, default_row.adrelid) = 'now()'
          END
), constraint_shapes AS (
    SELECT
        constraint_row.oid,
        constraint_row.conname,
        constraint_row.contype,
        constraint_row.conrelid,
        constraint_row.confrelid,
        constraint_row.confdeltype,
        constraint_row.convalidated,
        pg_get_constraintdef(constraint_row.oid, FALSE) AS definition,
        (
            SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
            FROM unnest(constraint_row.conkey) WITH ORDINALITY key_column(attnum, ordinality)
            JOIN pg_attribute attribute_row
              ON attribute_row.attrelid = constraint_row.conrelid
             AND attribute_row.attnum = key_column.attnum
        ) AS local_columns,
        (
            SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
            FROM unnest(constraint_row.confkey) WITH ORDINALITY key_column(attnum, ordinality)
            JOIN pg_attribute attribute_row
              ON attribute_row.attrelid = constraint_row.confrelid
             AND attribute_row.attnum = key_column.attnum
        ) AS referenced_columns
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid IN (
        (SELECT messages_oid FROM relation_oids),
        (SELECT mime_parts_oid FROM relation_oids),
        (SELECT classifications_oid FROM relation_oids),
        (SELECT projections_oid FROM relation_oids)
    )
)
SELECT CASE WHEN
    (SELECT COUNT(*) FROM matched_columns) = (SELECT COUNT(*) FROM expected_columns)
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT messages_oid FROM relation_oids)
          AND constraint_row.conname = 'messages_authorized_calendar_response_content_sha256_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%authorized_calendar_response_content_sha256 IS NULL%'
          AND constraint_row.definition LIKE '%^[0-9a-f]{64}$%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT messages_oid FROM relation_oids)
          AND constraint_row.conname = 'messages_calendar_response_processed_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%NOT calendar_response_processed%'
          AND constraint_row.definition LIKE '%authorized_calendar_response_content_sha256 IS NOT NULL%'
    )
    AND (
        SELECT COUNT(*) = 11
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'calendar_mail_classifications'
    )
    AND (
        SELECT COUNT(*) = 6
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'calendar_mail_classification_projections'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT mime_parts_oid FROM relation_oids)
          AND constraint_row.conname = 'mime_parts_scheduling_body_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%is_scheduling_body%'
          AND constraint_row.definition LIKE '%lower(btrim(split_part(content_type, '';''::text, 1))) = ''text/calendar''::text%'
          AND constraint_row.definition LIKE '%content_disposition IS DISTINCT FROM%attachment%'
          AND constraint_row.definition LIKE '%blob_id IS NOT NULL%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_index index_row
        JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
        WHERE index_row.indrelid = (SELECT mime_parts_oid FROM relation_oids)
          AND index_class.relname = 'mime_parts_one_scheduling_body_idx'
          AND index_row.indisunique
          AND index_row.indisvalid
          AND index_row.indisready
          AND index_row.indislive
          AND index_row.indnkeyatts = 2
          AND ARRAY[
                pg_get_indexdef(index_row.indexrelid, 1, FALSE),
                pg_get_indexdef(index_row.indexrelid, 2, FALSE)
              ] = ARRAY['tenant_id', 'message_id']::text[]
          AND pg_get_expr(index_row.indpred, index_row.indrelid, FALSE) = 'is_scheduling_body'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.contype = 'p'
          AND constraint_row.convalidated
          AND constraint_row.local_columns = ARRAY['tenant_id', 'message_id']::text[]
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_parser_revision_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%parser_revision > 0%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_generation_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%classification_generation > 0%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_classification_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%none%'
          AND constraint_row.definition LIKE '%request%'
          AND constraint_row.definition LIKE '%response%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_metadata_object_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%jsonb_typeof(metadata_json)%object%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_metadata_shape_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%needs_reclassification%'
          AND constraint_row.definition LIKE '%NOT needs_reclassification%'
          AND constraint_row.definition LIKE '%scheduling_mime_part_id IS NULL%'
          AND constraint_row.definition LIKE '%scheduling_mime_part_id IS NOT NULL%'
          AND constraint_row.definition LIKE '%metadata_json%kind%none%'
          AND constraint_row.definition LIKE '%metadata_json%request%IS TRUE%'
          AND constraint_row.definition LIKE '%metadata_json%response%IS TRUE%'
          AND constraint_row.definition LIKE '%jsonb_typeof%request%object%IS TRUE%'
          AND constraint_row.definition LIKE '%jsonb_typeof%response%object%IS TRUE%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_message_fkey'
          AND constraint_row.contype = 'f'
          AND constraint_row.convalidated
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.confrelid = (SELECT messages_oid FROM relation_oids)
          AND constraint_row.local_columns = ARRAY['tenant_id', 'message_id']::text[]
          AND constraint_row.referenced_columns = ARRAY['tenant_id', 'id']::text[]
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classifications_mime_part_fkey'
          AND constraint_row.contype = 'f'
          AND constraint_row.convalidated
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.confrelid = (SELECT mime_parts_oid FROM relation_oids)
          AND constraint_row.local_columns = ARRAY[
                'tenant_id', 'message_id', 'scheduling_mime_part_id'
              ]::text[]
          AND constraint_row.referenced_columns = ARRAY['tenant_id', 'message_id', 'id']::text[]
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT projections_oid FROM relation_oids)
          AND constraint_row.contype = 'p'
          AND constraint_row.convalidated
          AND constraint_row.local_columns = ARRAY[
                'tenant_id', 'account_id', 'message_id'
              ]::text[]
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT projections_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classification_projections_generation_check'
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.definition LIKE '%applied_generation > 0%'
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT projections_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classification_projections_account_fkey'
          AND constraint_row.contype = 'f'
          AND constraint_row.convalidated
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.confrelid = (SELECT accounts_oid FROM relation_oids)
          AND constraint_row.local_columns = ARRAY['tenant_id', 'account_id']::text[]
          AND constraint_row.referenced_columns = ARRAY['tenant_id', 'id']::text[]
    )
    AND EXISTS (
        SELECT 1
        FROM constraint_shapes constraint_row
        WHERE constraint_row.conrelid = (SELECT projections_oid FROM relation_oids)
          AND constraint_row.conname = 'calendar_mail_classification_projections_classification_fkey'
          AND constraint_row.contype = 'f'
          AND constraint_row.convalidated
          AND constraint_row.confdeltype = 'c'
          AND constraint_row.confrelid = (SELECT classifications_oid FROM relation_oids)
          AND constraint_row.local_columns = ARRAY['tenant_id', 'message_id']::text[]
          AND constraint_row.referenced_columns = ARRAY['tenant_id', 'message_id']::text[]
    )
THEN 1 ELSE 0 END;
