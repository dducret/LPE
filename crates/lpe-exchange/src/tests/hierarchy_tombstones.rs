use super::*;

const RETAINED_TOMBSTONE_COUNT: i64 = 1_001;

#[tokio::test]
async fn postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        WITH retained_folders AS (
            SELECT
                object_kind,
                series,
                md5(object_kind || ':' || series::text)::uuid AS canonical_id,
                CASE object_kind
                    WHEN 'mailbox' THEN 100000 + series
                    ELSE 200000 + series
                END::bigint AS global_counter
            FROM unnest(ARRAY['mailbox', 'search_folder_definition']) AS object_kind
            CROSS JOIN generate_series(1, $3::bigint) AS series
        )
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key,
            mapi_change_number, predecessor_change_list, deleted_at
        )
        SELECT
            $1,
            $2,
            object_kind,
            canonical_id,
            global_counter,
            (global_counter << 16) | 1,
            decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
                || decode(lpad(to_hex(global_counter), 12, '0'), 'hex'),
            decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
                || decode(lpad(to_hex(global_counter), 12, '0'), 'hex'),
            decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
                || decode(lpad(to_hex(global_counter), 12, '0'), 'hex'),
            global_counter,
            decode('16', 'hex')
                || decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
                || decode(lpad(to_hex(global_counter), 12, '0'), 'hex'),
            NOW()
        FROM retained_folders
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(RETAINED_TOMBSTONE_COUNT)
    .execute(storage.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        WITH retained_folders AS (
            SELECT
                object_kind,
                series,
                md5(object_kind || ':' || series::text)::uuid AS canonical_id,
                CASE object_kind
                    WHEN 'mailbox' THEN 100000 + series
                    ELSE 200000 + series
                END::bigint AS global_counter
            FROM unnest(ARRAY['mailbox', 'search_folder_definition']) AS object_kind
            CROSS JOIN generate_series(1, $3::bigint) AS series
        ), inserted_changes AS (
            INSERT INTO mail_change_log (
                tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
                modseq, affected_principal_ids, summary_json
            )
            SELECT
                $1,
                $2,
                CASE WHEN object_kind = 'mailbox' THEN canonical_id END,
                object_kind,
                canonical_id,
                'destroyed',
                global_counter,
                ARRAY[$2]::uuid[],
                '{}'::jsonb
            FROM retained_folders
            RETURNING object_kind, object_id, mailbox_id, modseq, cursor
        )
        INSERT INTO tombstones (
            id, tenant_id, account_id, mailbox_id, object_kind, object_id,
            deleted_modseq, change_cursor, reason, retained_until
        )
        SELECT
            md5('tombstone:' || object_kind || ':' || object_id::text)::uuid,
            $1,
            $2,
            mailbox_id,
            object_kind,
            object_id,
            modseq,
            cursor,
            'destroyed',
            NOW() + INTERVAL '30 days'
        FROM inserted_changes
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(RETAINED_TOMBSTONE_COUNT)
    .execute(storage.pool())
    .await
    .unwrap();

    let changes = storage
        .fetch_mapi_sync_changes(account_id, None, MapiCheckpointKind::Hierarchy, 0)
        .await
        .unwrap();

    assert_eq!(
        changes.deleted_mailbox_object_ids.len(),
        RETAINED_TOMBSTONE_COUNT as usize,
        "[MS-OXCFXICS] section 3.2.5.3 requires retained folders that have never been reported deleted to remain eligible beyond one storage page"
    );
    assert_eq!(
        changes.deleted_search_folder_object_ids.len(),
        RETAINED_TOMBSTONE_COUNT as usize,
        "search folders use the same retained hierarchy tombstone semantics"
    );
    assert_eq!(
        changes.deleted_mailbox_object_ids.last().copied(),
        Some(((100_000 + RETAINED_TOMBSTONE_COUNT as u64) << 16) | 1)
    );
    assert_eq!(
        changes.deleted_search_folder_object_ids.last().copied(),
        Some(((200_000 + RETAINED_TOMBSTONE_COUNT as u64) << 16) | 1)
    );

    fixture.cleanup().await.unwrap();
}
