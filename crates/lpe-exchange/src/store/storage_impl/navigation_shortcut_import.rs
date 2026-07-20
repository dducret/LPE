enum UnknownMapiNavigationShortcutDelete {
    AlreadyDeleted,
    Absent {
        object_id: u64,
        source_counter: u64,
        replica_guid: Uuid,
    },
}

async fn upsert_mapi_navigation_shortcut_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    input: UpsertMapiNavigationShortcutInput,
) -> Result<MapiNavigationShortcutRecord> {
    // [MS-OXCMSG] sections 2.2.3.2 and 2.2.3.3: each newly created
    // Message object has its own saved identity. [MS-OXOCFG] sections 2.2.9
    // and 4.4.2 model WLinks as Common Views FAI Message objects, so matching
    // target/type/section properties do not identify an existing WLink.
    let id = input.id.unwrap_or_else(Uuid::new_v4);
    let existed = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_navigation_shortcuts
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
        INSERT INTO mapi_navigation_shortcuts (
            tenant_id, id, account_id, subject, target_folder_id,
            shortcut_type, flags, save_stamp, section, ordinal, group_header_id, group_name,
            calendar_color, address_book_entry_id, address_book_store_entry_id,
            client_id, ro_group_type
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17)
        ON CONFLICT (tenant_id, id)
        DO UPDATE SET
            subject = EXCLUDED.subject,
            target_folder_id = EXCLUDED.target_folder_id,
            shortcut_type = EXCLUDED.shortcut_type,
            flags = EXCLUDED.flags,
            save_stamp = EXCLUDED.save_stamp,
            section = EXCLUDED.section,
            ordinal = EXCLUDED.ordinal,
            group_header_id = EXCLUDED.group_header_id,
            group_name = EXCLUDED.group_name,
            calendar_color = EXCLUDED.calendar_color,
            address_book_entry_id = EXCLUDED.address_book_entry_id,
            address_book_store_entry_id = EXCLUDED.address_book_store_entry_id,
            client_id = EXCLUDED.client_id,
            ro_group_type = EXCLUDED.ro_group_type,
            updated_at = NOW()
        RETURNING id, account_id, subject, target_folder_id, shortcut_type,
                  flags, save_stamp, section, ordinal, group_header_id, group_name,
                  calendar_color, address_book_entry_id,
                  address_book_store_entry_id, client_id, ro_group_type
        "#,
    )
    .bind(tenant_id)
    .bind(id)
    .bind(input.account_id)
    .bind(input.subject)
    .bind(input.target_folder_id.map(|value| value as i64))
    .bind(input.shortcut_type as i64)
    .bind(input.flags as i64)
    .bind(input.save_stamp as i64)
    .bind(input.section as i64)
    .bind(&input.ordinal)
    .bind(input.group_header_id)
    .bind(input.group_name)
    .bind(input.client_properties.calendar_color)
    .bind(input.client_properties.address_book_entry_id)
    .bind(input.client_properties.address_book_store_entry_id)
    .bind(input.client_properties.client_id)
    .bind(input.client_properties.ro_group_type)
    .fetch_one(&mut **tx)
    .await?;

    insert_mapi_navigation_shortcut_change(
        tx,
        tenant_id,
        input.account_id,
        id,
        if existed { "updated" } else { "created" },
    )
    .await?;
    mapi_navigation_shortcut_from_row(row)
}

async fn fetch_mapi_navigation_shortcut_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    canonical_id: Uuid,
) -> Result<MapiNavigationShortcutRecord> {
    let row = sqlx::query(
        r#"
        SELECT id, account_id, subject, target_folder_id, shortcut_type,
               flags, save_stamp, section, ordinal, group_header_id, group_name,
               calendar_color, address_book_entry_id,
               address_book_store_entry_id, client_id, ro_group_type
        FROM mapi_navigation_shortcuts
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("durable imported WLink content is missing"))?;
    mapi_navigation_shortcut_from_row(row)
}

async fn delete_mapi_navigation_shortcut_in_tx(
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
          AND object_kind = 'navigation_shortcut'
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
        anyhow::bail!("active MAPI navigation shortcut identity not found");
    }
    let deleted = sqlx::query_scalar::<_, Uuid>(
        r#"
        DELETE FROM mapi_navigation_shortcuts
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        RETURNING id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .fetch_optional(&mut **tx)
    .await?;
    if deleted.is_none() {
        anyhow::bail!("MAPI navigation shortcut not found");
    }
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET deleted_at = NOW(), updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'navigation_shortcut'
          AND canonical_id = $3
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .execute(&mut **tx)
    .await?;
    insert_mapi_navigation_shortcut_change(tx, tenant_id, account_id, canonical_id, "destroyed")
        .await?;
    Ok(())
}

async fn preflight_unknown_mapi_navigation_shortcut_delete_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    folder_id: u64,
    source_key: &[u8],
) -> Result<UnknownMapiNavigationShortcutDelete> {
    if folder_id != crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
        anyhow::bail!("unknown WLink tombstone is outside Common Views");
    }
    let object_id = crate::mapi::identity::object_id_from_source_key(source_key)
        .ok_or_else(|| anyhow::anyhow!("invalid deleted WLink SourceKey"))?;
    let source_counter = crate::mapi::identity::global_counter_from_store_id(object_id)
        .filter(|counter| {
            (crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                .contains(counter)
        })
        .ok_or_else(|| anyhow::anyhow!("deleted WLink SourceKey is outside the dynamic range"))?;
    let replica_guid = Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID);

    let existing = sqlx::query(
        r#"
        SELECT object_kind, canonical_id, mapi_object_id, source_key,
               deleted_at IS NOT NULL AS was_deleted
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND (mapi_object_id = $3 OR source_key = $4)
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(object_id as i64)
    .bind(source_key)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = existing {
        if row.get::<String, _>("object_kind") != "navigation_shortcut"
            || row.get::<i64, _>("mapi_object_id") as u64 != object_id
            || row.get::<Vec<u8>, _>("source_key") != source_key
        {
            anyhow::bail!("deleted WLink identity collides with another MAPI object");
        }
        if !row.get::<bool, _>("was_deleted") {
            anyhow::bail!("deleted WLink identity became active before tombstone commit");
        }
        // [MS-OXCFXICS] section 3.2.5.9.4.5: a request to delete an
        // object that is already deleted MUST be ignored. An online-created
        // WLink has a server-assigned identity and therefore no local-ID
        // reservation to validate on this idempotent retry path.
        return Ok(UnknownMapiNavigationShortcutDelete::AlreadyDeleted);
    }

    if !mapi_local_replica_counter_is_reserved_in_tx(
        tx,
        tenant_id,
        account_id,
        replica_guid,
        source_counter,
    )
    .await?
    {
        anyhow::bail!("deleted WLink SourceKey was not locally reserved");
    }

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
    .bind(source_key)
    .fetch_one(&mut **tx)
    .await?;
    if special_folder_alias_collision {
        anyhow::bail!("deleted WLink identity collides with a special-folder alias");
    }

    Ok(UnknownMapiNavigationShortcutDelete::Absent {
        object_id,
        source_counter,
        replica_guid,
    })
}

async fn tombstone_unknown_mapi_navigation_shortcut_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    folder_id: u64,
    source_key: &[u8],
) -> Result<()> {
    let UnknownMapiNavigationShortcutDelete::Absent {
        object_id,
        source_counter,
        replica_guid,
    } = preflight_unknown_mapi_navigation_shortcut_delete_in_tx(
        tx, tenant_id, account_id, folder_id, source_key,
    )
    .await?
    else {
        return Ok(());
    };

    let canonical_id = Uuid::new_v4();
    let mut change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    if change_number == source_counter {
        change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    }
    let change_key = crate::mapi::identity::change_key_for_change_number(change_number);
    let predecessor_change_list = crate::mapi_mailstore::predecessor_change_list(change_number);
    // [MS-OXCFXICS] section 3.2.5.9.4.5: retain a protocol tombstone
    // for an imported deletion of an object absent from this replica so
    // a later ImportMessageChange cannot restore it. This deliberately
    // writes no WLink content and no canonical change-log notification.
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list,
            deleted_at
        )
        VALUES (
            $1, $2, 'navigation_shortcut', $3,
            $4, $5, $6, $7, $8, $9, $10, NOW()
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .bind(source_counter as i64)
    .bind(object_id as i64)
    .bind(source_key)
    .bind(change_key)
    .bind(crate::mapi::identity::instance_key_for_object_id(object_id))
    .bind(change_number as i64)
    .bind(predecessor_change_list)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO mapi_local_replica_deleted_ranges (
            tenant_id, account_id, folder_id, replica_guid,
            min_global_counter, max_global_counter
        )
        VALUES ($1, $2, $3, $4, $5, $5)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(folder_id as i64)
    .bind(replica_guid)
    .bind(source_counter as i64)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
