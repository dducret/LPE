struct MapiImportedFaiIdentityCommit {
    canonical_id: Uuid,
    identity: MapiIdentityRecord,
    apply_imported_content: bool,
    disposition: MapiFaiImportDisposition,
}

fn imported_fai_version_wins_last_writer(
    incoming_last_modification_time: u64,
    incoming_change_key: &[u8],
    current_last_modification_time: u64,
    current_change_key: &[u8],
) -> Result<bool> {
    if !(17..=24).contains(&incoming_change_key.len())
        || !(17..=24).contains(&current_change_key.len())
    {
        anyhow::bail!("invalid imported FAI ChangeKey XID length");
    }
    // [MS-OXCFXICS] section 3.1.5.6.2.2: after comparing modification
    // times, equal versions are ordered by the ChangeKey NamespaceGuid.
    Ok(
        match incoming_last_modification_time.cmp(&current_last_modification_time) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => incoming_change_key[..16] >= current_change_key[..16],
        },
    )
}

async fn commit_mapi_imported_fai_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    object_kind: MapiIdentityObjectKind,
    folder_id: u64,
    requested_canonical_id: Option<Uuid>,
    imported: &MapiFaiImportedIdentity,
    fail_on_conflict: bool,
) -> Result<MapiImportedFaiIdentityCommit> {
    let object_id = crate::mapi::identity::object_id_from_source_key(&imported.source_key)
        .ok_or_else(|| anyhow::anyhow!("invalid imported FAI SourceKey"))?;
    let source_counter = crate::mapi::identity::global_counter_from_store_id(object_id)
        .filter(|counter| {
            (crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                .contains(counter)
        })
        .ok_or_else(|| anyhow::anyhow!("imported FAI SourceKey is outside the dynamic range"))?;
    if !(17..=24).contains(&imported.change_key.len()) {
        anyhow::bail!("invalid imported FAI ChangeKey XID length");
    }
    let imported_entries = parse_mapi_predecessor_change_list(&imported.predecessor_change_list)?;
    if serialize_mapi_predecessor_change_list(&imported_entries)?
        != imported.predecessor_change_list
        || !mapi_predecessors_contain_change_key(&imported_entries, &imported.change_key)?
    {
        anyhow::bail!("imported FAI PCL is not canonical or does not contain its ChangeKey");
    }
    let last_modification_time =
        imported.last_modification_time - imported.last_modification_time % 10;
    let last_modification_time_i64 = i64::try_from(last_modification_time)
        .map_err(|_| anyhow::anyhow!("invalid imported FAI modification time"))?;

    let existing = sqlx::query(
        r#"
        SELECT object_kind, canonical_id, mapi_object_id, mapi_change_number,
               source_key, change_key, predecessor_change_list,
               deleted_at IS NOT NULL AS was_deleted,
               to_char(
                   updated_at AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) AS updated_at
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
    .bind(&imported.source_key)
    .fetch_optional(&mut **tx)
    .await?;
    let canonical_id = existing
        .as_ref()
        .map(|row| row.get::<Uuid, _>("canonical_id"))
        .or(requested_canonical_id)
        .unwrap_or_else(Uuid::new_v4);

    if let Some(row) = existing {
        if row.get::<String, _>("object_kind") != object_kind.as_str()
            || row.get::<i64, _>("mapi_object_id") as u64 != object_id
            || row.get::<Vec<u8>, _>("source_key") != imported.source_key
        {
            anyhow::bail!("imported FAI identity collides with another MAPI object");
        }
        let current_change_number = row.get::<i64, _>("mapi_change_number") as u64;
        let was_deleted = row.get::<bool, _>("was_deleted");
        if was_deleted {
            // [MS-OXCFXICS] section 3.3.4.3.3.2.2.1 allows ObjectDeleted
            // from SaveChangesMessage when the imported object was deleted.
            // Preserve the tombstoned SourceKey so a replay cannot resurrect
            // canonical Common Views content.
            return Err(MapiFaiImportObjectDeleted.into());
        }
        let current_change_key = row.get::<Vec<u8>, _>("change_key");
        let current_predecessor_change_list = row.get::<Vec<u8>, _>("predecessor_change_list");
        let current_last_modification_time =
            crate::mapi_mailstore::filetime_from_rfc3339_utc(&row.get::<String, _>("updated_at"));
        let current_entries = parse_mapi_predecessor_change_list(&current_predecessor_change_list)?;
        if mapi_predecessors_include(&current_entries, &imported_entries)?
            && mapi_predecessors_contain_change_key(&current_entries, &imported.change_key)?
        {
            return Ok(MapiImportedFaiIdentityCommit {
                canonical_id,
                identity: MapiIdentityRecord {
                    object_kind,
                    canonical_id,
                    object_id,
                    change_number: current_change_number,
                    source_key: imported.source_key.clone(),
                    change_key: current_change_key,
                    predecessor_change_list: current_predecessor_change_list,
                    last_modification_time: current_last_modification_time,
                },
                apply_imported_content: false,
                disposition: MapiFaiImportDisposition::IgnoredOlderOrSame,
            });
        }

        let conflict = !mapi_predecessors_include(&imported_entries, &current_entries)?;
        if conflict && fail_on_conflict {
            return Err(MapiFaiImportConflict.into());
        }
        let mut change_number =
            allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
        if change_number == source_counter {
            change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
        }
        let mut merged_entries = current_entries;
        merge_mapi_predecessors(&mut merged_entries, imported_entries)?;
        merge_mapi_predecessor_change_key(&mut merged_entries, &imported.change_key)?;
        let predecessor_change_list = serialize_mapi_predecessor_change_list(&merged_entries)?;
        let imported_wins = !conflict
            || imported_fai_version_wins_last_writer(
                last_modification_time,
                &imported.change_key,
                current_last_modification_time,
                &current_change_key,
            )?;
        let change_key = if imported_wins {
            imported.change_key.clone()
        } else {
            current_change_key
        };
        let resolved_last_modification_time = if imported_wins {
            last_modification_time
        } else {
            current_last_modification_time
        };
        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_change_number = $5,
                change_key = $6,
                predecessor_change_list = $7,
                deleted_at = NULL,
                updated_at = TIMESTAMPTZ '1601-01-01 00:00:00+00'
                    + (($8::bigint / 10) * INTERVAL '1 microsecond')
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $9
              AND canonical_id = $3
              AND mapi_object_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(canonical_id)
        .bind(object_id as i64)
        .bind(change_number as i64)
        .bind(&change_key)
        .bind(&predecessor_change_list)
        .bind(resolved_last_modification_time as i64)
        .bind(object_kind.as_str())
        .execute(&mut **tx)
        .await?;
        return Ok(MapiImportedFaiIdentityCommit {
            canonical_id,
            identity: MapiIdentityRecord {
                object_kind,
                canonical_id,
                object_id,
                change_number,
                source_key: imported.source_key.clone(),
                change_key,
                predecessor_change_list,
                last_modification_time: resolved_last_modification_time,
            },
            apply_imported_content: imported_wins,
            disposition: if conflict {
                MapiFaiImportDisposition::ConflictResolved { imported_wins }
            } else {
                MapiFaiImportDisposition::Applied
            },
        });
    }

    if mapi_local_replica_counter_is_deleted_in_folder_in_tx(
        tx,
        tenant_id,
        account_id,
        folder_id,
        Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID),
        source_counter,
    )
    .await?
    {
        // [MS-OXCFXICS] sections 3.2.5.9.4.5 and 3.2.5.9.4.8:
        // neither ImportMessageChange nor its later Save may resurrect a
        // SourceKey already present in the folder's deleted-item list.
        return Err(MapiFaiImportObjectDeleted.into());
    }

    let canonical_identity_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_object_identities
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $4
              AND canonical_id = $3
              AND deleted_at IS NULL
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(canonical_id)
    .bind(object_kind.as_str())
    .fetch_one(&mut **tx)
    .await?;
    if canonical_identity_exists {
        anyhow::bail!("imported FAI canonical identity has a different SourceKey");
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
    .bind(&imported.source_key)
    .fetch_one(&mut **tx)
    .await?;
    if special_folder_alias_collision {
        anyhow::bail!("imported FAI identity collides with a special-folder alias");
    }

    // [MS-OXCFXICS] sections 3.3.5.2.1 and 3.3.5.2.2: only a new
    // client-chosen local MID requires a durable GetLocalReplicaIds range.
    // An existing online-created Message has a server-assigned identity, so
    // resolve and lock it above before applying the new-object reservation
    // rule. Section 3.2.5.9.4.2 permits that existing object to be updated.
    if !mapi_local_replica_counter_is_reserved_in_tx(
        tx,
        tenant_id,
        account_id,
        Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID),
        source_counter,
    )
    .await?
    {
        anyhow::bail!("imported FAI SourceKey was not locally reserved");
    }

    let mut change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    if change_number == source_counter {
        change_number = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
    }
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list, updated_at
        )
        VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8, $9, $10, $11,
            TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + (($12::bigint / 10) * INTERVAL '1 microsecond')
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(object_kind.as_str())
    .bind(canonical_id)
    .bind(source_counter as i64)
    .bind(object_id as i64)
    .bind(&imported.source_key)
    .bind(&imported.change_key)
    .bind(crate::mapi::identity::instance_key_for_object_id(object_id))
    .bind(change_number as i64)
    .bind(&imported.predecessor_change_list)
    .bind(last_modification_time_i64)
    .execute(&mut **tx)
    .await?;

    // [MS-OXCFXICS] sections 2.2.1.2.7, 2.2.1.2.8, and 3.1.5.3:
    // retain the imported MID/SourceKey/ChangeKey/PCL and assign a distinct
    // server-internal CN as one durable Common Views FAI transaction.
    Ok(MapiImportedFaiIdentityCommit {
        canonical_id,
        identity: MapiIdentityRecord {
            object_kind,
            canonical_id,
            object_id,
            change_number,
            source_key: imported.source_key.clone(),
            change_key: imported.change_key.clone(),
            predecessor_change_list: imported.predecessor_change_list.clone(),
            last_modification_time,
        },
        apply_imported_content: true,
        disposition: MapiFaiImportDisposition::Applied,
    })
}

