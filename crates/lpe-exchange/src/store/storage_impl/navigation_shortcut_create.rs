async fn commit_mapi_navigation_shortcut_create_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    mut input: CommitMapiNavigationShortcutCreateInput,
) -> Result<MapiNavigationShortcutCommit> {
    let account_id = input.shortcut.account_id;
    let canonical_id = input.shortcut.id.unwrap_or_else(Uuid::new_v4);
    input.shortcut.id = Some(canonical_id);

    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO NOTHING
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;

    // [MS-OXCFXICS] section 3.3.5.2.1 permits a client-chosen local ID only
    // for ImportMessageChange. Section 3.3.5.2.2 requires the server to
    // assign the identity for an online ROP create, so PidTagSourceKey from
    // RopSetProperties is never an input to this commit.
    let global_counter = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    let object_id = crate::mapi::identity::mapi_store_id(global_counter);
    let source_key = crate::mapi::identity::source_key_for_object_id(object_id);
    let change_number = global_counter;
    let change_key = crate::mapi::identity::change_key_for_change_number(change_number);
    let predecessor_change_list = crate::mapi_mailstore::predecessor_change_list(change_number);
    let instance_key = crate::mapi::identity::instance_key_for_object_id(object_id);

    let special_folder_alias_collision = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_special_folder_aliases
            WHERE tenant_id = $1
              AND account_id = $2
              AND (alias_folder_id = $3 OR source_key = $4)
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(object_id as i64)
    .bind(&source_key)
    .fetch_one(&mut **tx)
    .await?;
    if special_folder_alias_collision {
        anyhow::bail!("native WLink identity collides with a special-folder alias");
    }

    let identity_row = sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'navigation_shortcut', $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING to_char(
            updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ) AS updated_at
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .bind(global_counter as i64)
    .bind(object_id as i64)
    .bind(&source_key)
    .bind(&change_key)
    .bind(instance_key)
    .bind(change_number as i64)
    .bind(&predecessor_change_list)
    .fetch_one(&mut **tx)
    .await?;
    let identity = MapiIdentityRecord {
        object_kind: MapiIdentityObjectKind::NavigationShortcut,
        canonical_id,
        object_id,
        change_number,
        source_key,
        change_key,
        predecessor_change_list,
        last_modification_time: crate::mapi_mailstore::filetime_from_rfc3339_utc(
            &identity_row.get::<String, _>("updated_at"),
        ),
    };

    // [MS-OXOCFG] section 3.1.4.10: the WLink Save publishes its content
    // and message identity together; any identity failure rolls both back.
    let shortcut = upsert_mapi_navigation_shortcut_in_tx(tx, tenant_id, input.shortcut).await?;
    Ok(MapiNavigationShortcutCommit { shortcut, identity })
}
