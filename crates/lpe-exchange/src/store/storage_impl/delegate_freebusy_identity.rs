const FETCH_LOCAL_FREEBUSY_IDENTITY_SQL: &str = r#"
    SELECT mapi_object_id,
           mapi_change_number,
           source_key,
           change_key,
           predecessor_change_list,
           to_char(
               updated_at AT TIME ZONE 'UTC',
               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
           ) AS updated_at
    FROM mapi_object_identities
    WHERE tenant_id = $1
      AND account_id = $2
      AND object_kind = 'delegate_freebusy_message'
      AND canonical_id = $3
      AND deleted_at IS NULL
    FOR UPDATE
"#;

fn local_freebusy_identity_from_row(row: sqlx::postgres::PgRow) -> MapiIdentityRecord {
    MapiIdentityRecord {
        object_kind: MapiIdentityObjectKind::DelegateFreeBusyMessage,
        canonical_id: crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID,
        object_id: row.get::<i64, _>("mapi_object_id") as u64,
        change_number: row.get::<i64, _>("mapi_change_number") as u64,
        source_key: row.get("source_key"),
        change_key: row.get("change_key"),
        predecessor_change_list: row.get("predecessor_change_list"),
        last_modification_time: crate::mapi_mailstore::filetime_from_rfc3339_utc(
            &row.get::<String, _>("updated_at"),
        ),
    }
}

macro_rules! store_impl_delegate_freebusy_identity {
    () => {
        fn fetch_local_freebusy_projection<'a>(
            &'a self,
            account_id: Uuid,
        ) -> StoreFuture<'a, MapiLocalFreebusyProjection> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let mut tx = self.pool().begin().await?;

                // Keep one acquisition order for every account: store singleton,
                // mailbox replica, projection marker, then LocalFreebusy identity.
                let store_identity =
                    mapi_store_identity_for_account_in_tx(&mut tx, tenant_id, account_id).await?;
                sqlx::query(
                    r#"
                    INSERT INTO delegation_projection_state (tenant_id, account_id)
                    VALUES ($1, $2)
                    ON CONFLICT (tenant_id, account_id) DO NOTHING
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .execute(&mut *tx)
                .await?;
                let (revision, applied_revision) = sqlx::query_as::<_, (i64, i64)>(
                    r#"
                    SELECT revision, applied_revision
                    FROM delegation_projection_state
                    WHERE tenant_id = $1
                      AND account_id = $2
                    FOR UPDATE
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await?;

                let canonical_id = crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID;
                let existing = sqlx::query(FETCH_LOCAL_FREEBUSY_IDENTITY_SQL)
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(canonical_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                let identity = if let Some(row) = existing {
                    if revision > applied_revision {
                        let current_change_key = row.get::<Vec<u8>, _>("change_key");
                        let mut predecessors = parse_mapi_predecessor_change_list(
                            &row.get::<Vec<u8>, _>("predecessor_change_list"),
                        )?;
                        if !mapi_predecessors_contain_change_key(
                            &predecessors,
                            &current_change_key,
                        )? {
                            anyhow::bail!(
                                "LocalFreebusy PCL does not contain its current ChangeKey"
                            );
                        }
                        let change_number =
                            allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id)
                                .await?;
                        let change_key = lpe_storage::mapi_store_identity::mapi_xid(
                            store_identity.replica_guid,
                            change_number,
                        );
                        merge_mapi_predecessor_change_key(&mut predecessors, &change_key)?;
                        let predecessor_change_list =
                            serialize_mapi_predecessor_change_list(&predecessors)?;
                        sqlx::query(
                            r#"
                            UPDATE mapi_object_identities
                            SET mapi_change_number = $4,
                                change_key = $5,
                                predecessor_change_list = $6,
                                updated_at = GREATEST(
                                    clock_timestamp(),
                                    updated_at + INTERVAL '1 microsecond'
                                )
                            WHERE tenant_id = $1
                              AND account_id = $2
                              AND object_kind = 'delegate_freebusy_message'
                              AND canonical_id = $3
                              AND deleted_at IS NULL
                            "#,
                        )
                        .bind(tenant_id)
                        .bind(account_id)
                        .bind(canonical_id)
                        .bind(change_number as i64)
                        .bind(change_key)
                        .bind(predecessor_change_list)
                        .execute(&mut *tx)
                        .await?;
                        let row = sqlx::query(FETCH_LOCAL_FREEBUSY_IDENTITY_SQL)
                            .bind(tenant_id)
                            .bind(account_id)
                            .bind(canonical_id)
                            .fetch_one(&mut *tx)
                            .await?;
                        local_freebusy_identity_from_row(row)
                    } else {
                        local_freebusy_identity_from_row(row)
                    }
                } else {
                    let global_counter =
                        allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?;
                    let (object_id, source_key, change_key, instance_key, predecessor_change_list) =
                        mapi_identity_material_for_store_replica(
                            store_identity.replica_guid,
                            global_counter,
                        );
                    let alias_collision = sqlx::query_scalar::<_, bool>(
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
                    .fetch_one(&mut *tx)
                    .await?;
                    if alias_collision {
                        anyhow::bail!(
                            "MAPI LocalFreebusy identity collides with a special-folder alias"
                        );
                    }
                    let row = sqlx::query(
                        r#"
                        INSERT INTO mapi_object_identities (
                            tenant_id, account_id, object_kind, canonical_id,
                            mapi_global_counter, mapi_object_id, source_key,
                            change_key, instance_key, mapi_change_number,
                            predecessor_change_list
                        )
                        VALUES (
                            $1, $2, 'delegate_freebusy_message', $3,
                            $4, $5, $6, $7, $8, $4, $9
                        )
                        RETURNING mapi_object_id,
                                  mapi_change_number,
                                  source_key,
                                  change_key,
                                  predecessor_change_list,
                                  to_char(
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
                    .bind(source_key)
                    .bind(change_key)
                    .bind(instance_key)
                    .bind(predecessor_change_list)
                    .fetch_one(&mut *tx)
                    .await?;
                    local_freebusy_identity_from_row(row)
                };

                // The marker lock makes this plain MVCC read and the identity
                // version one coherent canonical projection.
                let delegate_rows = sqlx::query(FETCH_EWS_DELEGATES_SQL)
                    .bind(tenant_id)
                    .bind(account_id)
                    .fetch_all(&mut *tx)
                    .await?;
                let delegates = ews_delegates_from_rows(account_id, delegate_rows)?;
                let applied_revision = sqlx::query_scalar::<_, i64>(
                    r#"
                    UPDATE delegation_projection_state
                    SET applied_revision = revision
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND revision = $3
                    RETURNING applied_revision
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(revision)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("LocalFreebusy projection revision changed"))?;
                if applied_revision != revision {
                    anyhow::bail!("LocalFreebusy projection revision was not applied");
                }
                tx.commit().await?;
                Ok(MapiLocalFreebusyProjection {
                    identity,
                    delegates,
                })
            })
        }
    };
}
