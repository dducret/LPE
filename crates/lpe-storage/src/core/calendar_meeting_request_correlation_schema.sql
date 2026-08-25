SELECT EXISTS (
    SELECT 1
    FROM pg_index index_row
    JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
    JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
    JOIN pg_class table_class ON table_class.oid = index_row.indrelid
    JOIN pg_am access_method ON access_method.oid = index_class.relam
    WHERE index_namespace.nspname = $1
      AND index_class.relname = 'calendar_events_active_uid_correlation_idx'
      AND table_class.relname = 'calendar_events'
      AND access_method.amname = 'btree'
      AND index_row.indisvalid
      AND index_row.indisready
      AND NOT index_row.indisunique
      AND index_row.indnatts = 4
      AND index_row.indnkeyatts = 4
      AND (
          SELECT array_agg(attribute_row.attname::text ORDER BY key_row.ordinality)
          FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY
               AS key_row(attnum, ordinality)
          JOIN pg_attribute attribute_row
            ON attribute_row.attrelid = index_row.indrelid
           AND attribute_row.attnum = key_row.attnum
          WHERE key_row.ordinality <= index_row.indnkeyatts
      ) = ARRAY['tenant_id', 'owner_account_id', 'uid', 'id']::text[]
      AND pg_get_expr(index_row.indpred, index_row.indrelid) =
          '(lifecycle_state = ''active''::text)'
)
AND EXISTS (
    SELECT 1
    FROM pg_attribute attribute_row
    JOIN pg_class table_row ON table_row.oid = attribute_row.attrelid
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    JOIN pg_attrdef default_row
      ON default_row.adrelid = attribute_row.attrelid
     AND default_row.adnum = attribute_row.attnum
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'calendar_events'
      AND attribute_row.attname = 'projection_state'
      AND NOT attribute_row.attisdropped
      AND attribute_row.attnotnull
      AND pg_get_expr(default_row.adbin, default_row.adrelid) = '''visible''::text'
)
AND EXISTS (
    SELECT 1
    FROM pg_constraint constraint_row
    JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = $1
      AND table_row.relname = 'calendar_events'
      AND constraint_row.conname = 'calendar_events_projection_state_check'
      AND constraint_row.contype = 'c'
      AND constraint_row.convalidated
      AND pg_get_constraintdef(constraint_row.oid)
          LIKE '%projection_state%visible%mapi_submission_placeholder%'
)
