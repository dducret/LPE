macro_rules! store_impl_mapi_special_folder_aliases {
    () => {
        fn fetch_mapi_special_folder_aliases<'a>(
            &'a self,
            account_id: Uuid,
        ) -> StoreFuture<'a, Vec<MapiSpecialFolderAlias>> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let rows = sqlx::query(
                    r#"
                    SELECT alias_folder_id, canonical_folder_id, source_key
                    FROM mapi_special_folder_aliases
                    WHERE tenant_id = $1
                      AND account_id = $2
                    ORDER BY alias_folder_id
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .fetch_all(self.pool())
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| MapiSpecialFolderAlias {
                        alias_folder_id: row.get::<i64, _>("alias_folder_id") as u64,
                        canonical_folder_id: row.get::<i64, _>("canonical_folder_id") as u64,
                        source_key: row.get("source_key"),
                    })
                    .collect())
            })
        }

        fn upsert_mapi_special_folder_aliases<'a>(
            &'a self,
            account_id: Uuid,
            aliases: &'a [MapiSpecialFolderAlias],
        ) -> StoreFuture<'a, Vec<u64>> {
            Box::pin(async move {
                if aliases.is_empty() {
                    return Ok(Vec::new());
                }
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let mut tx = self.pool().begin().await?;
                let reserved_end = sqlx::query_scalar::<_, i64>(
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
                .ok_or_else(|| anyhow::anyhow!("MAPI mailbox replica was not initialized"))?
                    as u64;
                let mut change_numbers = Vec::with_capacity(aliases.len());
                for alias in aliases {
                    let alias_counter =
                        crate::mapi::identity::global_counter_from_store_id(alias.alias_folder_id);
                    if alias.alias_folder_id == alias.canonical_folder_id
                        || crate::mapi::identity::global_counter_from_store_id(
                            alias.canonical_folder_id,
                        )
                        .is_none_or(|counter| {
                            counter >= crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                        })
                        || alias_counter.is_none_or(|counter| {
                            !(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                                ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                                .contains(&counter)
                                || counter >= reserved_end
                        })
                        || crate::mapi::identity::object_id_from_source_key(&alias.source_key)
                            != Some(alias.alias_folder_id)
                    {
                        anyhow::bail!("invalid MAPI special-folder alias");
                    }
                    let existing_alias = sqlx::query(
                        r#"
                        SELECT alias_folder_id, canonical_folder_id, source_key,
                               mapi_change_number
                        FROM mapi_special_folder_aliases
                        WHERE tenant_id = $1
                          AND account_id = $2
                          AND (alias_folder_id = $3 OR source_key = $4)
                        LIMIT 1
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(alias.alias_folder_id as i64)
                    .bind(&alias.source_key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    if let Some(existing) = existing_alias {
                        if existing.get::<i64, _>("alias_folder_id") as u64 == alias.alias_folder_id
                            && existing.get::<i64, _>("canonical_folder_id") as u64
                                == alias.canonical_folder_id
                            && existing.get::<Vec<u8>, _>("source_key") == alias.source_key
                        {
                            change_numbers
                                .push(existing.get::<i64, _>("mapi_change_number") as u64);
                            continue;
                        }
                        anyhow::bail!("conflicting MAPI special-folder alias");
                    }
                    let identity_collision = sqlx::query_scalar::<_, bool>(
                        r#"
                        SELECT EXISTS (
                            SELECT 1
                            FROM mapi_object_identities
                            WHERE tenant_id = $1
                              AND account_id = $2
                              AND (
                                  mapi_object_id = $3
                                  OR source_key = $4
                              )
                        )
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(alias.alias_folder_id as i64)
                    .bind(&alias.source_key)
                    .fetch_one(&mut *tx)
                    .await?;
                    if identity_collision {
                        anyhow::bail!("MAPI special-folder alias collides with an object identity");
                    }
                    // [MS-OXCFXICS] sections 3.2.5.9.4.3 and 3.3.5.2.1 require a
                    // new server CN and forbid using the client-reserved FID as that CN.
                    let change_number =
                        allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?;
                    sqlx::query(
                        r#"
                        INSERT INTO mapi_special_folder_aliases (
                            tenant_id,
                            account_id,
                            alias_folder_id,
                            canonical_folder_id,
                            source_key,
                            mapi_change_number
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(alias.alias_folder_id as i64)
                    .bind(alias.canonical_folder_id as i64)
                    .bind(&alias.source_key)
                    .bind(change_number as i64)
                    .execute(&mut *tx)
                    .await?;
                    change_numbers.push(change_number);
                }
                tx.commit().await?;
                Ok(change_numbers)
            })
        }
    };
}
