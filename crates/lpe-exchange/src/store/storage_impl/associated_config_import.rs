async fn fetch_mapi_associated_config_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    config_id: Uuid,
) -> Result<MapiAssociatedConfigRecord> {
    let row = sqlx::query(
        r#"
        SELECT id, account_id, folder_id, message_class, subject, properties_json,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                   AS updated_at
        FROM mapi_associated_config_messages
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(config_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("durable imported associated config content is missing"))?;
    mapi_associated_config_from_row(row)
}

async fn upsert_mapi_associated_config_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    input: UpsertMapiAssociatedConfigInput,
) -> Result<MapiAssociatedConfigRecord> {
    let id = input
        .id
        .ok_or_else(|| anyhow::anyhow!("associated config canonical ID is missing"))?;
    let existed = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_associated_config_messages
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        )
        "#,
    )
    .bind(tenant_id)
    .bind(input.account_id)
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    let row = sqlx::query(
        r#"
        INSERT INTO mapi_associated_config_messages (
            tenant_id, id, account_id, folder_id, message_class, subject, properties_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant_id, id)
        DO UPDATE SET
            folder_id = EXCLUDED.folder_id,
            message_class = EXCLUDED.message_class,
            subject = EXCLUDED.subject,
            properties_json = EXCLUDED.properties_json,
            updated_at = NOW()
        WHERE mapi_associated_config_messages.account_id = EXCLUDED.account_id
        RETURNING id, account_id, folder_id, message_class, subject, properties_json,
                  to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                      AS updated_at
        "#,
    )
    .bind(tenant_id)
    .bind(id)
    .bind(input.account_id)
    .bind(input.folder_id as i64)
    .bind(input.message_class)
    .bind(input.subject)
    .bind(input.properties_json)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("MAPI associated config message not found"))?;
    let saved = mapi_associated_config_from_row(row)?;
    insert_mapi_associated_config_change(
        tx,
        tenant_id,
        input.account_id,
        saved.id,
        if existed { "updated" } else { "created" },
        saved.folder_id,
    )
    .await?;
    Ok(saved)
}

async fn commit_mapi_associated_config_update_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    input: UpsertMapiAssociatedConfigInput,
) -> Result<MapiAssociatedConfigCommit> {
    let canonical_id = input
        .id
        .ok_or_else(|| anyhow::anyhow!("existing associated config canonical identity is missing"))?;
    let account_id = input.account_id;
    let current = sqlx::query(
        r#"
        SELECT identity.mapi_object_id, identity.source_key, identity.change_key,
               identity.predecessor_change_list
        FROM mapi_object_identities identity
        JOIN mapi_associated_config_messages config
          ON config.tenant_id = identity.tenant_id
         AND config.account_id = identity.account_id
         AND config.id = identity.canonical_id
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'associated_config'
          AND identity.canonical_id = $3
          AND identity.deleted_at IS NULL
        FOR UPDATE OF identity, config
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("active MAPI associated config identity was not found"))?;

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
        anyhow::bail!("existing associated config PCL does not contain its current ChangeKey");
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
          AND object_kind = 'associated_config'
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

    // [MS-OXCPRPT] sections 3.1.1 and 3.2.5.4, 3.2.5.5, and 3.2.5.13
    // plus [MS-OXCROPS] section 2.2.6.3: Message and Stream mutations are
    // handle-local until RopSaveChangesMessage publishes content and version.
    let config = upsert_mapi_associated_config_in_tx(tx, tenant_id, input).await?;
    Ok(MapiAssociatedConfigCommit {
        config,
        identity: MapiIdentityRecord {
            object_kind: MapiIdentityObjectKind::AssociatedConfig,
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

async fn delete_mapi_associated_config_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    canonical_id: Uuid,
) -> Result<()> {
    let identity = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT canonical_id
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'associated_config'
          AND canonical_id = $3
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?;
    if identity.is_none() {
        anyhow::bail!("active MAPI associated config identity not found");
    }

    let deleted = sqlx::query_scalar::<_, i64>(
        r#"
        DELETE FROM mapi_associated_config_messages
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        RETURNING folder_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("MAPI associated config message not found"))?;

    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET deleted_at = NOW(), updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'associated_config'
          AND canonical_id = $3
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .execute(&mut **tx)
    .await?;

    insert_mapi_associated_config_change(
        tx,
        tenant_id,
        account_id,
        canonical_id,
        "destroyed",
        deleted as u64,
    )
    .await?;
    Ok(())
}
