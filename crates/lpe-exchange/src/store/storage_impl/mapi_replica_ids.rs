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
                tx.commit().await?;
                Ok(first_global_counter as u64)
            })
        }
    };
}
