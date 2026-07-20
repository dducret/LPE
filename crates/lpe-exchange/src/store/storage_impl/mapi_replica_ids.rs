macro_rules! store_impl_mapi_replica_ids {
    () => {
        fn reserve_mapi_local_replica_ids<'a>(
            &'a self,
            account_id: Uuid,
            id_count: u32,
        ) -> StoreFuture<'a, u64> {
            Box::pin(async move {
                if !(1..=MAX_MAPI_LOCAL_REPLICA_ID_COUNT).contains(&id_count) {
                    anyhow::bail!("invalid MAPI local replica ID count: {id_count}");
                }

                let tenant_id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    SELECT tenant_id
                    FROM accounts
                    WHERE id = $1
                    LIMIT 1
                    "#,
                )
                .bind(account_id)
                .fetch_optional(self.pool())
                .await?
                .ok_or_else(|| anyhow::anyhow!("account not found"))?;
                let mut tx = self.pool().begin().await?;
                sqlx::query(
                    r#"
                    INSERT INTO mapi_mailbox_replicas (
                        tenant_id, account_id, replica_guid, next_global_counter
                    )
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tenant_id, account_id)
                    DO UPDATE SET
                        next_global_counter = GREATEST(
                            mapi_mailbox_replicas.next_global_counter,
                            $4
                        )
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
                .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
                .execute(&mut *tx)
                .await?;

                // [MS-OXCFXICS] sections 2.2.3.2.4.7 and 3.3.5.2.1: the
                // complete range returned to the local replica is exclusive.
                let first_global_counter = sqlx::query_scalar::<_, i64>(
                    r#"
                    WITH allocated_floor AS (
                        SELECT GREATEST(
                            COALESCE(
                                MAX(GREATEST(
                                    identities.mapi_global_counter,
                                    identities.mapi_change_number
                                )) + 1,
                                $4
                            ),
                            $4
                        ) AS value
                        FROM mapi_object_identities identities
                        WHERE identities.tenant_id = $1
                          AND identities.account_id = $2
                          AND identities.mapi_global_counter < $5
                          AND identities.mapi_change_number < $5
                    ), reservation AS (
                        UPDATE mapi_mailbox_replicas replica
                        SET next_global_counter = GREATEST(
                                replica.next_global_counter,
                                allocated_floor.value,
                                $4
                            ) + $6,
                            updated_at = NOW()
                        FROM allocated_floor
                        WHERE replica.tenant_id = $1
                          AND replica.account_id = $2
                          AND replica.replica_guid = $3
                          AND GREATEST(
                                replica.next_global_counter,
                                allocated_floor.value,
                                $4
                              ) <= $5 - $6
                        RETURNING replica.next_global_counter - $6 AS first_global_counter
                    )
                    SELECT first_global_counter
                    FROM reservation
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
                .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
                .bind(crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
                .bind(i64::from(id_count))
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "MAPI local replica ID space exhausted or replica GUID mismatch"
                    )
                })?;
                let end_global_counter_exclusive = first_global_counter + i64::from(id_count);
                // [MS-OXCFXICS] sections 2.2.3.2.4.7, 3.2.5.9.4.7,
                // and 3.3.5.2.1: retain every exact range returned to the
                // local replica. The allocation high-water cannot prove that
                // an arbitrary lower counter was actually reserved.
                sqlx::query(
                    r#"
                    INSERT INTO mapi_local_replica_id_ranges (
                        tenant_id, account_id, replica_guid,
                        first_global_counter, end_global_counter_exclusive
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
                .bind(first_global_counter)
                .bind(end_global_counter_exclusive)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(first_global_counter as u64)
            })
        }

        fn add_mapi_local_replica_deleted_ranges<'a>(
            &'a self,
            account_id: Uuid,
            folder_id: u64,
            ranges: &'a [MapiLocalReplicaDeletedRange],
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                let folder_id = i64::try_from(folder_id)
                    .ok()
                    .filter(|folder_id| *folder_id > 0)
                    .ok_or_else(|| anyhow::anyhow!("invalid MAPI folder ID"))?;
                if ranges.is_empty() {
                    anyhow::bail!("invalid MAPI local replica deleted-item range request");
                }
                let tenant_id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    SELECT tenant_id
                    FROM accounts
                    WHERE id = $1
                    LIMIT 1
                    "#,
                )
                .bind(account_id)
                .fetch_optional(self.pool())
                .await?
                .ok_or_else(|| anyhow::anyhow!("account not found"))?;
                let mut tx = self.pool().begin().await?;
                sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT next_global_counter
                    FROM mapi_mailbox_replicas
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND replica_guid = $3
                    FOR UPDATE
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MAPI mailbox replica was not initialized"))?;

                for range in ranges {
                    if range.replica_guid
                        != Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID)
                        || range.min_global_counter > range.max_global_counter
                        || !(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                            ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                            .contains(&range.min_global_counter)
                        || !(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                            ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                            .contains(&range.max_global_counter)
                        || !mapi_local_replica_range_is_reserved_in_tx(
                            &mut tx,
                            tenant_id,
                            account_id,
                            range,
                        )
                        .await?
                    {
                        anyhow::bail!(
                            "MAPI local replica deleted-item range was not previously reserved"
                        );
                    }
                }

                // [MS-OXCFXICS] sections 2.2.3.2.4.8.1 and 3.2.5.9.4.8:
                // add each validated folder-scoped range to the durable
                // deleted-item list. Replays are idempotent.
                for range in ranges {
                    sqlx::query(
                        r#"
                        INSERT INTO mapi_local_replica_deleted_ranges (
                            tenant_id, account_id, folder_id, replica_guid,
                            min_global_counter, max_global_counter
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(folder_id)
                    .bind(range.replica_guid)
                    .bind(range.min_global_counter as i64)
                    .bind(range.max_global_counter as i64)
                    .execute(&mut *tx)
                    .await?;
                }
                tx.commit().await?;
                Ok(())
            })
        }
    };
}

async fn mapi_local_replica_counter_is_reserved_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    replica_guid: Uuid,
    global_counter: u64,
) -> Result<bool> {
    let global_counter = i64::try_from(global_counter)
        .map_err(|_| anyhow::anyhow!("invalid MAPI local replica global counter"))?;
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_local_replica_id_ranges
            WHERE tenant_id = $1
              AND account_id = $2
              AND replica_guid = $3
              AND first_global_counter <= $4
              AND end_global_counter_exclusive > $4
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(replica_guid)
    .bind(global_counter)
    .fetch_one(&mut **tx)
    .await?)
}

async fn mapi_local_replica_counter_is_deleted_in_folder_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    folder_id: u64,
    replica_guid: Uuid,
    global_counter: u64,
) -> Result<bool> {
    let folder_id = i64::try_from(folder_id)
        .map_err(|_| anyhow::anyhow!("invalid MAPI folder ID"))?;
    let global_counter = i64::try_from(global_counter)
        .map_err(|_| anyhow::anyhow!("invalid MAPI local replica global counter"))?;
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_local_replica_deleted_ranges
            WHERE tenant_id = $1
              AND account_id = $2
              AND folder_id = $3
              AND replica_guid = $4
              AND min_global_counter <= $5
              AND max_global_counter >= $5
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(folder_id)
    .bind(replica_guid)
    .bind(global_counter)
    .fetch_one(&mut **tx)
    .await?)
}

async fn mapi_local_replica_range_is_reserved_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    range: &MapiLocalReplicaDeletedRange,
) -> Result<bool> {
    let min_global_counter = i64::try_from(range.min_global_counter)
        .map_err(|_| anyhow::anyhow!("invalid minimum MAPI local replica global counter"))?;
    let max_global_counter = i64::try_from(range.max_global_counter)
        .map_err(|_| anyhow::anyhow!("invalid maximum MAPI local replica global counter"))?;
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT COALESCE(
            SUM(
                LEAST(end_global_counter_exclusive, $5 + 1)
                - GREATEST(first_global_counter, $4)
            ),
            0
        ) = $5 - $4 + 1
        FROM mapi_local_replica_id_ranges
        WHERE tenant_id = $1
          AND account_id = $2
          AND replica_guid = $3
          AND first_global_counter <= $5
          AND end_global_counter_exclusive > $4
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(range.replica_guid)
    .bind(min_global_counter)
    .bind(max_global_counter)
    .fetch_one(&mut **tx)
    .await?)
}
