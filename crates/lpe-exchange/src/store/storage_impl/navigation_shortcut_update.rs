async fn commit_mapi_navigation_shortcut_update_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    input: UpsertMapiNavigationShortcutInput,
) -> Result<MapiNavigationShortcutCommit> {
    let canonical_id = input
        .id
        .ok_or_else(|| anyhow::anyhow!("existing WLink canonical identity is missing"))?;
    let account_id = input.account_id;
    let current = sqlx::query(
        r#"
        SELECT identity.mapi_object_id, identity.mapi_change_number,
               identity.source_key, identity.change_key,
               identity.predecessor_change_list
        FROM mapi_object_identities identity
        JOIN mapi_navigation_shortcuts shortcut
          ON shortcut.tenant_id = identity.tenant_id
         AND shortcut.account_id = identity.account_id
         AND shortcut.id = identity.canonical_id
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.canonical_id = $3
          AND identity.deleted_at IS NULL
        FOR UPDATE OF identity, shortcut
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("active MAPI navigation shortcut identity was not found"))?;

    let object_id = current.get::<i64, _>("mapi_object_id") as u64;
    let source_key = current.get::<Vec<u8>, _>("source_key");
    let current_change_key = current.get::<Vec<u8>, _>("change_key");
    let mut predecessors = parse_mapi_predecessor_change_list(
        &current.get::<Vec<u8>, _>("predecessor_change_list"),
    )?;
    let change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    let change_key = crate::mapi::identity::change_key_for_change_number(change_number);
    merge_mapi_predecessor_change_key(&mut predecessors, &change_key)?;
    let predecessor_change_list = serialize_mapi_predecessor_change_list(&predecessors)?;
    if !mapi_predecessors_contain_change_key(&predecessors, &current_change_key)? {
        anyhow::bail!("existing WLink PCL does not contain its current ChangeKey");
    }

    let identity_row = sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_change_number = $4,
            change_key = $5,
            predecessor_change_list = $6,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'navigation_shortcut'
          AND canonical_id = $3
          AND deleted_at IS NULL
        RETURNING to_char(
            updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ) AS updated_at
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .bind(change_number as i64)
    .bind(&change_key)
    .bind(&predecessor_change_list)
    .fetch_one(&mut **tx)
    .await?;

    // [MS-OXOCFG] section 3.1.4.10 and [MS-OXCROPS] sections 2.2.8.6
    // and 2.2.6.3: only RopSaveChangesMessage publishes the staged WLink.
    // Content and its new CN/CK/PCL/LMT are committed in one transaction.
    let shortcut = upsert_mapi_navigation_shortcut_in_tx(tx, tenant_id, input).await?;
    Ok(MapiNavigationShortcutCommit {
        shortcut,
        identity: MapiIdentityRecord {
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
        },
    })
}
