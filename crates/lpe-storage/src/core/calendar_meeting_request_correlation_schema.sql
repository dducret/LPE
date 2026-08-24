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
