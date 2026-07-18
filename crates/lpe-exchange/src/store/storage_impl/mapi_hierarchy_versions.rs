macro_rules! store_impl_mapi_hierarchy_versions {
    () => {
        fn commit_mapi_folder_hierarchy_change<'a>(
            &'a self,
            account_id: Uuid,
            folder_id: u64,
            imported_last_modification_time: i64,
            imported_change_key: &'a [u8],
            imported_predecessor_change_list: &'a [u8],
        ) -> StoreFuture<'a, MapiFolderHierarchyCommitOutcome> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let imported_last_modification_time =
                    u64::try_from(imported_last_modification_time)
                        .map_err(|_| anyhow::anyhow!("invalid MAPI hierarchy modification time"))?;
                // PostgreSQL timestamps have microsecond precision. Normalize
                // FILETIME once so every later projection and LWW comparison
                // uses the exact durable value.
                let imported_last_modification_time =
                    imported_last_modification_time - imported_last_modification_time % 10;
                let imported_entries = parse_mapi_predecessor_change_list(
                    imported_predecessor_change_list,
                )?;
                if !mapi_predecessors_contain_change_key(
                    &imported_entries,
                    imported_change_key,
                )? {
                    anyhow::bail!("MAPI hierarchy PCL does not contain its ChangeKey");
                }

                let mut tx = self.pool().begin().await?;
                let row = sqlx::query(
                    r#"
                    SELECT canonical_id,
                           mapi_change_number,
                           change_key,
                           predecessor_change_list,
                           to_char(
                               updated_at AT TIME ZONE 'UTC',
                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                           ) AS updated_at
                    FROM mapi_object_identities
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = 'mailbox'
                      AND mapi_object_id = $3
                      AND deleted_at IS NULL
                    FOR UPDATE
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(folder_id as i64)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MAPI folder identity not found"))?;
                let canonical_id = row.get::<Uuid, _>("canonical_id");
                let current_change_number = row.get::<i64, _>("mapi_change_number") as u64;
                let current_change_key = row.get::<Vec<u8>, _>("change_key");
                let current_predecessor_change_list =
                    row.get::<Vec<u8>, _>("predecessor_change_list");
                let current_last_modification_time = crate::mapi_mailstore::filetime_from_rfc3339_utc(
                    &row.get::<String, _>("updated_at"),
                );
                let current_entries =
                    parse_mapi_predecessor_change_list(&current_predecessor_change_list)?;

                // [MS-OXCFXICS] section 3.3.4.3.3.1.1: an already imported
                // change is idempotent. Do not consume another server CN when
                // a retry's complete ancestry is already present.
                if mapi_predecessors_include(&current_entries, &imported_entries)?
                    && mapi_predecessors_contain_change_key(
                        &current_entries,
                        imported_change_key,
                    )?
                {
                    tx.commit().await?;
                    return Ok(MapiFolderHierarchyCommitOutcome::Duplicate(
                        MapiFolderVersion {
                            folder_id,
                            change_number: current_change_number,
                            change_key: current_change_key,
                            predecessor_change_list: current_predecessor_change_list,
                            last_modification_time: current_last_modification_time,
                        },
                    ));
                }

                let conflict = !mapi_predecessors_include(&imported_entries, &current_entries)?;

                let change_number =
                    allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?;
                let mut merged_entries = current_entries;
                merge_mapi_predecessors(&mut merged_entries, imported_entries)?;
                merge_mapi_predecessor_change_key(
                    &mut merged_entries,
                    imported_change_key,
                )?;
                let predecessor_change_list =
                    serialize_mapi_predecessor_change_list(&merged_entries)?;
                // [MS-OXCFXICS] sections 3.1.5.3 and 3.1.5.6.2.2: the
                // imported version receives a server CN, but keeps the client
                // ChangeKey. For a hierarchy conflict, resolve by last
                // modification time; an equal timestamp keeps the server
                // version, and the successor PCL integrates both lineages.
                let imported_wins = !conflict
                    || imported_last_modification_time > current_last_modification_time;
                let resolved_change_key = if imported_wins {
                    imported_change_key.to_vec()
                } else {
                    current_change_key
                };
                let resolved_last_modification_time = if imported_wins {
                    imported_last_modification_time
                } else {
                    current_last_modification_time
                };

                let updated = sqlx::query(
                    r#"
                    UPDATE mapi_object_identities
                    SET mapi_change_number = $4,
                        change_key = $5,
                        predecessor_change_list = $6,
                        updated_at = TIMESTAMPTZ '1601-01-01 00:00:00+00'
                            + (($7::bigint / 10) * INTERVAL '1 microsecond')
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = 'mailbox'
                      AND mapi_object_id = $3
                      AND deleted_at IS NULL
                    RETURNING to_char(
                        updated_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    ) AS updated_at
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(folder_id as i64)
                .bind(change_number as i64)
                .bind(&resolved_change_key)
                .bind(&predecessor_change_list)
                .bind(resolved_last_modification_time as i64)
                .fetch_optional(&mut *tx)
                .await?;
                let updated = updated
                    .ok_or_else(|| anyhow::anyhow!("MAPI folder identity changed during hierarchy commit"))?;
                let resolved_last_modification_time =
                    crate::mapi_mailstore::filetime_from_rfc3339_utc(
                        &updated.get::<String, _>("updated_at"),
                    );
                let modseq = sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT COALESCE(MAX(modseq), 0) + 1
                    FROM mail_change_log
                    WHERE tenant_id = $1 AND account_id = $2
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await?;
                sqlx::query(
                    r#"
                    INSERT INTO mail_change_log (
                        tenant_id, account_id, mailbox_id, object_kind, object_id,
                        change_kind, modseq, affected_principal_ids, summary_json
                    )
                    VALUES (
                        $1, $2, $3, 'mailbox', $3, 'updated', $4,
                        ARRAY[$2]::uuid[],
                        jsonb_build_object(
                            'mapiOnly', TRUE,
                            'mapiFolderId', $5::text,
                            'mapiChangeNumber', $6::text
                        )
                    )
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(canonical_id)
                .bind(modseq)
                .bind(folder_id as i64)
                .bind(change_number as i64)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                let version = MapiFolderVersion {
                    folder_id,
                    change_number,
                    change_key: resolved_change_key,
                    predecessor_change_list,
                    last_modification_time: resolved_last_modification_time,
                };
                if conflict {
                    Ok(MapiFolderHierarchyCommitOutcome::Conflict(version))
                } else {
                    Ok(MapiFolderHierarchyCommitOutcome::Applied(version))
                }
            })
        }

    };
}

type MapiPredecessors = std::collections::BTreeMap<[u8; 16], Vec<u8>>;

fn parse_mapi_predecessor_change_list(bytes: &[u8]) -> Result<MapiPredecessors> {
    let mut entries = MapiPredecessors::new();
    let mut offset = 0usize;
    let mut previous_guid = None;
    while offset < bytes.len() {
        let size = usize::from(
            *bytes
                .get(offset)
                .ok_or_else(|| anyhow::anyhow!("truncated MAPI PCL SizedXid"))?,
        );
        offset += 1;
        if !(17..=24).contains(&size) {
            anyhow::bail!("invalid MAPI PCL XID length");
        }
        let end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("MAPI PCL XID length overflow"))?;
        let xid = bytes
            .get(offset..end)
            .ok_or_else(|| anyhow::anyhow!("truncated MAPI PCL XID"))?;
        offset = end;
        let guid: [u8; 16] = xid[..16].try_into().expect("validated XID GUID");
        if previous_guid.is_some_and(|previous| previous >= guid) {
            anyhow::bail!("MAPI PCL XIDs are not strictly sorted by replica GUID");
        }
        previous_guid = Some(guid);
        entries.insert(guid, xid[16..].to_vec());
    }
    if entries.is_empty() {
        anyhow::bail!("MAPI PCL must contain at least one SizedXid");
    }
    Ok(entries)
}

fn mapi_predecessors_contain_change_key(
    entries: &MapiPredecessors,
    change_key: &[u8],
) -> Result<bool> {
    if !(17..=24).contains(&change_key.len()) {
        anyhow::bail!("invalid MAPI ChangeKey XID length");
    }
    let guid: [u8; 16] = change_key[..16].try_into().expect("validated ChangeKey GUID");
    let Some(local_id) = entries.get(&guid) else {
        return Ok(false);
    };
    if local_id.len() != change_key.len() - 16 {
        anyhow::bail!("MAPI PCL LocalIds for one replica have inconsistent lengths");
    }
    Ok(local_id.as_slice() >= &change_key[16..])
}

fn mapi_predecessors_include(
    current: &MapiPredecessors,
    incoming: &MapiPredecessors,
) -> Result<bool> {
    for (guid, incoming_local_id) in incoming {
        let Some(current_local_id) = current.get(guid) else {
            return Ok(false);
        };
        if current_local_id.len() != incoming_local_id.len() {
            anyhow::bail!("MAPI PCL LocalIds for one replica have inconsistent lengths");
        }
        if current_local_id < incoming_local_id {
            return Ok(false);
        }
    }
    Ok(true)
}

fn merge_mapi_predecessors(
    current: &mut MapiPredecessors,
    incoming: MapiPredecessors,
) -> Result<()> {
    for (guid, local_id) in incoming {
        match current.get_mut(&guid) {
            Some(current_local_id) => {
                if current_local_id.len() != local_id.len() {
                    anyhow::bail!("MAPI PCL LocalIds for one replica have inconsistent lengths");
                }
                if *current_local_id < local_id {
                    *current_local_id = local_id;
                }
            }
            None => {
                current.insert(guid, local_id);
            }
        }
    }
    Ok(())
}

fn merge_mapi_predecessor_change_key(
    entries: &mut MapiPredecessors,
    change_key: &[u8],
) -> Result<()> {
    if !(17..=24).contains(&change_key.len()) {
        anyhow::bail!("invalid MAPI ChangeKey XID length");
    }
    let guid: [u8; 16] = change_key[..16].try_into().expect("validated ChangeKey GUID");
    let local_id = change_key[16..].to_vec();
    merge_mapi_predecessors(entries, std::iter::once((guid, local_id)).collect())
}

fn serialize_mapi_predecessor_change_list(entries: &MapiPredecessors) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for (guid, local_id) in entries {
        let size = guid.len().saturating_add(local_id.len());
        bytes.push(u8::try_from(size).map_err(|_| anyhow::anyhow!("MAPI PCL XID is too large"))?);
        bytes.extend_from_slice(guid);
        bytes.extend_from_slice(local_id);
    }
    Ok(bytes)
}

#[cfg(test)]
mod mapi_hierarchy_version_tests {
    use super::*;

    #[test]
    fn predecessor_merge_is_idempotent_and_keeps_both_replicas() {
        let server = crate::mapi::identity::change_key_for_change_number(8);
        let client = [
            0x51, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0,
            0x5e, 0x0d, 0xa6, 0x00, 0x00, 0x04, 0x15,
        ];
        let mut current = parse_mapi_predecessor_change_list(
            &crate::mapi_mailstore::predecessor_change_list(8),
        )
        .unwrap();
        merge_mapi_predecessor_change_key(&mut current, &client).unwrap();

        assert!(mapi_predecessors_contain_change_key(&current, &server).unwrap());
        assert!(mapi_predecessors_contain_change_key(&current, &client).unwrap());
        let serialized = serialize_mapi_predecessor_change_list(&current).unwrap();
        assert_eq!(parse_mapi_predecessor_change_list(&serialized).unwrap(), current);
    }

    #[test]
    fn stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version() {
        let current = parse_mapi_predecessor_change_list(
            &crate::mapi_mailstore::predecessor_change_list(47),
        )
        .unwrap();
        let mut imported = parse_mapi_predecessor_change_list(
            &crate::mapi_mailstore::predecessor_change_list(8),
        )
        .unwrap();
        let outlook_change_key = [
            0x51, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0,
            0x5e, 0x0d, 0xa6, 0x00, 0x00, 0x04, 0x15,
        ];
        merge_mapi_predecessor_change_key(&mut imported, &outlook_change_key).unwrap();

        assert!(!mapi_predecessors_include(&imported, &current).unwrap());
    }
}
