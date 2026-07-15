macro_rules! store_impl_mapi_metadata {
    () => {
    fn fetch_or_allocate_mapi_identities<'a>(
        &'a self,
        account_id: Uuid,
        requests: &'a [MapiIdentityRequest],
    ) -> StoreFuture<'a, Vec<MapiIdentityRecord>> {
        Box::pin(async move {
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

            let preserved_mailbox_identity_ids =
                mapi_collaboration_folder_identity_ids_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            sqlx::query(
                r#"
                INSERT INTO mapi_mailbox_replicas (
                    tenant_id,
                    account_id,
                    replica_guid,
                    next_global_counter
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id, account_id)
                DO UPDATE SET
                    next_global_counter = GREATEST(
                        mapi_mailbox_replicas.next_global_counter,
                        $4
                    ),
                    updated_at = CASE
                        WHEN mapi_mailbox_replicas.next_global_counter < $4 THEN NOW()
                        ELSE mapi_mailbox_replicas.updated_at
                    END
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
            .execute(&mut *tx)
            .await?;
            advance_mapi_replica_counter_past_allocated(&mut tx, tenant_id, account_id).await?;
            repair_reserved_mapi_identity_counter_collisions(&mut tx, tenant_id, account_id)
                .await?;
            repair_reserved_mapi_mailbox_identities(&mut tx, tenant_id, account_id).await?;
            repair_invalid_mapi_identity_material(&mut tx, tenant_id, account_id).await?;
            repair_stale_mapi_object_identities(
                &mut tx,
                tenant_id,
                account_id,
                &preserved_mailbox_identity_ids,
            )
            .await?;

            let mut records = Vec::with_capacity(requests.len());
            for request in requests {
                let kind = request.object_kind.as_str();
                let existing = sqlx::query(
                    r#"
                    SELECT mapi_object_id, source_key
                    FROM mapi_object_identities
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = $3
                      AND canonical_id = $4
                      AND deleted_at IS NULL
                    LIMIT 1
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(kind)
                .bind(request.canonical_id)
                .fetch_optional(&mut *tx)
                .await?;

                let (object_id, source_key) = if let Some(row) = existing {
                    (
                        row.get::<i64, _>("mapi_object_id") as u64,
                        row.get("source_key"),
                    )
                } else {
                    let global_counter = if let Some(counter) = request.reserved_global_counter {
                        counter
                    } else {
                        allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?
                    };
                    let (object_id, default_source_key, change_key, instance_key) =
                        crate::mapi::identity::persisted_identity_material(global_counter);
                    let predecessor_change_list =
                        crate::mapi_mailstore::predecessor_change_list(global_counter);
                    let source_key = request.source_key.clone().unwrap_or(default_source_key);
                    let row = sqlx::query(
                        r#"
                        INSERT INTO mapi_object_identities (
                            tenant_id,
                            account_id,
                            object_kind,
                            canonical_id,
                            mapi_global_counter,
                            mapi_object_id,
                            source_key,
                            change_key,
                            instance_key,
                            mapi_change_number,
                            predecessor_change_list
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (tenant_id, account_id, object_kind, canonical_id)
                        DO UPDATE SET
                            deleted_at = NULL,
                            updated_at = CASE
                                WHEN mapi_object_identities.deleted_at IS NULL
                                THEN mapi_object_identities.updated_at
                                ELSE NOW()
                            END
                        RETURNING mapi_object_id, source_key
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(kind)
                    .bind(request.canonical_id)
                    .bind(global_counter as i64)
                    .bind(object_id as i64)
                    .bind(source_key)
                    .bind(change_key)
                    .bind(instance_key)
                    .bind(global_counter as i64)
                    .bind(predecessor_change_list)
                    .fetch_one(&mut *tx)
                    .await?;
                    (
                        row.get::<i64, _>("mapi_object_id") as u64,
                        row.get("source_key"),
                    )
                };
                records.push(MapiIdentityRecord {
                    object_kind: request.object_kind,
                    canonical_id: request.canonical_id,
                    object_id,
                    source_key,
                });
            }
            tx.commit().await?;
            Ok(records)
        })
    }

    fn fetch_mapi_identities_by_object_ids<'a>(
        &'a self,
        account_id: Uuid,
        object_ids: &'a [u64],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        Box::pin(async move {
            if object_ids.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let object_ids = object_ids
                .iter()
                .map(|value| *value as i64)
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT object_kind, canonical_id, mapi_object_id, source_key
                FROM mapi_object_identities
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mapi_object_id = ANY($3)
                  AND deleted_at IS NULL
                  AND (
                    object_kind <> 'mailbox'
                    OR mapi_global_counter < $4
                    OR EXISTS (
                        SELECT 1
                        FROM mailboxes mailbox
                        WHERE mailbox.tenant_id = mapi_object_identities.tenant_id
                          AND mailbox.account_id = mapi_object_identities.account_id
                          AND mailbox.id = mapi_object_identities.canonical_id
                    )
                  )
                  AND (
                    object_kind <> 'search_folder_definition'
                    OR EXISTS (
                        SELECT 1
                        FROM search_folders search_folder
                        WHERE search_folder.tenant_id = mapi_object_identities.tenant_id
                          AND search_folder.account_id = mapi_object_identities.account_id
                          AND search_folder.id = mapi_object_identities.canonical_id
                    )
                  )
                  AND (
                    object_kind <> 'associated_config'
                    OR EXISTS (
                        SELECT 1
                        FROM mapi_associated_config_messages config
                        WHERE config.tenant_id = mapi_object_identities.tenant_id
                          AND config.account_id = mapi_object_identities.account_id
                          AND config.id = mapi_object_identities.canonical_id
                          AND (
                              config.folder_id = ANY($5::bigint[])
                              OR EXISTS (
                                  SELECT 1
                                  FROM mapi_object_identities folder_identity
                                  WHERE folder_identity.tenant_id = config.tenant_id
                                    AND folder_identity.account_id = config.account_id
                                    AND folder_identity.mapi_object_id = config.folder_id
                                    AND folder_identity.object_kind IN ('mailbox', 'search_folder_definition')
                                    AND folder_identity.deleted_at IS NULL
                              )
                          )
                    )
                  )
                  AND (
                    object_kind <> 'navigation_shortcut'
                    OR EXISTS (
                        SELECT 1
                        FROM mapi_navigation_shortcuts shortcut
                        WHERE shortcut.tenant_id = mapi_object_identities.tenant_id
                          AND shortcut.account_id = mapi_object_identities.account_id
                          AND shortcut.id = mapi_object_identities.canonical_id
                    )
                  )
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(&object_ids)
            .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
            .bind(MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS.as_slice())
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_identity_lookup_from_row)
                .collect()
        })
    }

    fn fetch_mapi_object_ids_for_deleted_changes<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiIdentityObjectKind,
        canonical_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<u64>> {
        Box::pin(async move {
            if canonical_ids.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT mapi_object_id
                FROM mapi_object_identities
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = ANY($4)
                ORDER BY mapi_object_id
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_ids)
            .fetch_all(self.pool())
            .await?;

            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let object_id = row.get::<i64, _>("mapi_object_id");
                    (object_id > 0).then_some(object_id as u64)
                })
                .collect())
        })
    }

    fn fetch_mapi_identities_by_source_keys<'a>(
        &'a self,
        account_id: Uuid,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        Box::pin(async move {
            if source_keys.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT object_kind, canonical_id, mapi_object_id, source_key
                FROM mapi_object_identities
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND source_key = ANY($3)
                  AND deleted_at IS NULL
                  AND (
                    object_kind <> 'mailbox'
                    OR mapi_global_counter < $4
                    OR EXISTS (
                        SELECT 1
                        FROM mailboxes mailbox
                        WHERE mailbox.tenant_id = mapi_object_identities.tenant_id
                          AND mailbox.account_id = mapi_object_identities.account_id
                          AND mailbox.id = mapi_object_identities.canonical_id
                    )
                  )
                  AND (
                    object_kind <> 'search_folder_definition'
                    OR EXISTS (
                        SELECT 1
                        FROM search_folders search_folder
                        WHERE search_folder.tenant_id = mapi_object_identities.tenant_id
                          AND search_folder.account_id = mapi_object_identities.account_id
                          AND search_folder.id = mapi_object_identities.canonical_id
                    )
                  )
                  AND (
                    object_kind <> 'associated_config'
                    OR EXISTS (
                        SELECT 1
                        FROM mapi_associated_config_messages config
                        WHERE config.tenant_id = mapi_object_identities.tenant_id
                          AND config.account_id = mapi_object_identities.account_id
                          AND config.id = mapi_object_identities.canonical_id
                          AND (
                              config.folder_id = ANY($5::bigint[])
                              OR EXISTS (
                                  SELECT 1
                                  FROM mapi_object_identities folder_identity
                                  WHERE folder_identity.tenant_id = config.tenant_id
                                    AND folder_identity.account_id = config.account_id
                                    AND folder_identity.mapi_object_id = config.folder_id
                                    AND folder_identity.object_kind IN ('mailbox', 'search_folder_definition')
                                    AND folder_identity.deleted_at IS NULL
                              )
                          )
                    )
                  )
                  AND (
                    object_kind <> 'navigation_shortcut'
                    OR EXISTS (
                        SELECT 1
                        FROM mapi_navigation_shortcuts shortcut
                        WHERE shortcut.tenant_id = mapi_object_identities.tenant_id
                          AND shortcut.account_id = mapi_object_identities.account_id
                          AND shortcut.id = mapi_object_identities.canonical_id
                    )
                  )
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_keys)
            .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
            .bind(MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS.as_slice())
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_identity_lookup_from_row)
                .collect()
        })
    }

    fn fetch_or_allocate_mapi_named_property_ids<'a>(
        &'a self,
        account_id: Uuid,
        properties: &'a [MapiNamedProperty],
        create: bool,
    ) -> StoreFuture<'a, Vec<Option<MapiNamedPropertyMapping>>> {
        Box::pin(async move {
            if properties.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            for _attempt in 0..8 {
                let mut tx = self.pool().begin().await?;
                let mut mappings = Vec::with_capacity(properties.len());
                let mut retry = false;

                for property in properties {
                    let property = normalize_mapi_named_property(property.clone());
                    if let Some(mapping) =
                        fetch_mapi_named_property_in_tx(&mut tx, tenant_id, account_id, &property)
                            .await?
                    {
                        mappings.push(Some(mapping));
                        continue;
                    }
                    if !create {
                        mappings.push(None);
                        continue;
                    }

                    let property_id =
                        allocate_next_mapi_named_property_id(&mut tx, tenant_id, account_id)
                            .await?;
                    match insert_mapi_named_property_in_tx(
                        &mut tx,
                        tenant_id,
                        account_id,
                        property_id,
                        &property,
                    )
                    .await
                    {
                        Ok(()) => mappings.push(Some(MapiNamedPropertyMapping {
                            property_id,
                            property,
                        })),
                        Err(error) if is_unique_violation(&error) => {
                            retry = true;
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }

                if retry {
                    tx.rollback().await?;
                    continue;
                }

                tx.commit().await?;
                return Ok(mappings);
            }
            Err(anyhow::anyhow!(
                "MAPI named property allocation conflicted repeatedly"
            ))
        })
    }

    fn fetch_mapi_named_properties_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        property_ids: &'a [u16],
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            if property_ids.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let ids = property_ids
                .iter()
                .map(|id| i32::from(*id))
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT property_id, property_guid, property_kind, property_lid, property_name
                FROM mapi_named_properties
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND property_id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_named_property_mapping_from_row)
                .collect()
        })
    }

    fn fetch_mapi_named_properties<'a>(
        &'a self,
        account_id: Uuid,
        guid: Option<[u8; 16]>,
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let guid = guid.map(Vec::from);
            let rows = sqlx::query(
                r#"
                SELECT property_id, property_guid, property_kind, property_lid, property_name
                FROM mapi_named_properties
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND ($3::bytea IS NULL OR property_guid = $3)
                ORDER BY property_id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(guid)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_named_property_mapping_from_row)
                .collect()
        })
    }

    fn upsert_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        values: &'a [MapiCustomPropertyValue],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if values.is_empty() {
                return Ok(());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            for value in values {
                sqlx::query(
                    r#"
                    INSERT INTO mapi_custom_property_values (
                        tenant_id,
                        account_id,
                        object_kind,
                        canonical_id,
                        property_tag,
                        property_type,
                        property_value
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (
                        tenant_id,
                        account_id,
                        object_kind,
                        canonical_id,
                        property_tag,
                        property_type
                    )
                    DO UPDATE SET
                        property_value = EXCLUDED.property_value,
                        updated_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(object_kind.as_str())
                .bind(canonical_id)
                .bind(i64::from(value.property_tag))
                .bind(i32::from(value.property_type))
                .bind(&value.property_value)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn fetch_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>> {
        Box::pin(async move {
            if property_tags.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let tags = property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT property_tag, property_type, property_value
                FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = $4
                  AND property_tag = ANY($5)
                ORDER BY property_tag, property_type
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_id)
            .bind(&tags)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_custom_property_value_from_row)
                .collect()
        })
    }

    fn fetch_all_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT property_tag, property_type, property_value
                FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = $4
                ORDER BY property_tag, property_type
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_custom_property_value_from_row)
                .collect()
        })
    }

    fn delete_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if property_tags.is_empty() {
                return Ok(());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let tags = property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            sqlx::query(
                r#"
                DELETE FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = $4
                  AND property_tag = ANY($5)
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_id)
            .bind(&tags)
            .execute(self.pool())
            .await?;
            Ok(())
        })
    }

    fn fetch_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        folder_id: u64,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiFolderProfilePropertyValue>> {
        Box::pin(async move {
            if property_tags.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let tags = property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT folder_id, property_tag, property_type, property_value
                FROM mapi_folder_profile_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND folder_id = $3
                  AND property_tag = ANY($4)
                ORDER BY property_tag, property_type
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(folder_id as i64)
            .bind(&tags)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_folder_profile_property_value_from_row)
                .collect()
        })
    }

    fn upsert_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        values: &'a [MapiFolderProfilePropertyValue],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if values.is_empty() {
                return Ok(());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            for value in values {
                if value.property_value.is_empty() || value.property_value.len() > 4096 {
                    anyhow::bail!("invalid MAPI folder profile property value");
                }
                sqlx::query(
                    r#"
                    INSERT INTO mapi_folder_profile_property_values (
                        tenant_id,
                        account_id,
                        folder_id,
                        property_tag,
                        property_type,
                        property_value
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (
                        tenant_id,
                        account_id,
                        folder_id,
                        property_tag,
                        property_type
                    )
                    DO UPDATE SET
                        property_value = EXCLUDED.property_value,
                        updated_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(value.folder_id as i64)
                .bind(i64::from(value.property_tag))
                .bind(i32::from(value.property_type))
                .bind(&value.property_value)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let row = sqlx::query(
                r#"
                SELECT mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND expires_at > NOW()
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(mailbox_id)
            .fetch_optional(self.pool())
            .await?;

            row.map(mapi_sync_checkpoint_from_row).transpose()
        })
    }

    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            let existing = sqlx::query(
                r#"
                SELECT id, mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json,
                       expires_at > NOW() AS checkpoint_is_live
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(mailbox_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(existing) = existing.as_ref() {
                let existing_change_sequence =
                    existing.get::<i64, _>("last_change_sequence").max(0) as u64;
                let existing_modseq = existing.get::<i64, _>("last_modseq").max(0) as u64;
                let checkpoint_is_live = existing.get::<bool, _>("checkpoint_is_live");
                if checkpoint_is_live
                    && (existing_change_sequence > last_change_sequence
                    || (existing_change_sequence == last_change_sequence
                        && existing_modseq > last_modseq))
                {
                    let checkpoint = MapiSyncCheckpoint {
                        mailbox_id: existing.get::<Option<Uuid>, _>("mailbox_id"),
                        checkpoint_kind,
                        last_change_sequence: existing_change_sequence,
                        last_modseq: existing_modseq,
                        cursor_json: existing.get("cursor_json"),
                    };
                    tx.commit().await?;
                    return Ok(checkpoint);
                }
            }
            let existing_id = existing.as_ref().map(|row| row.get::<Uuid, _>("id"));
            let row = sqlx::query(
                if existing_id.is_some() {
                    r#"
                    UPDATE mapi_sync_checkpoints
                    SET
                        last_change_sequence = $7,
                        last_modseq = $8,
                        cursor_json = $9,
                        updated_at = NOW(),
                        expires_at = NOW() + INTERVAL '30 days'
                    WHERE id = $1
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                } else {
                    r#"
                    INSERT INTO mapi_sync_checkpoints (
                        id, tenant_id, account_id, mailbox_id, checkpoint_kind,
                        mapi_replica_guid, last_change_sequence, last_modseq,
                        cursor_json, expires_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() + INTERVAL '30 days')
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                },
            )
            .bind(existing_id.unwrap_or_else(Uuid::new_v4))
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(last_change_sequence as i64)
            .bind(last_modseq as i64)
            .bind(cursor_json)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;

            mapi_sync_checkpoint_from_row(row)
        })
    }

    fn fetch_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { Storage::fetch_mapi_ipm_subtree_ost_id(self, account_id).await })
    }

    fn store_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
        ost_id: &'a [u8],
    ) -> StoreFuture<'a, ()> {
        Box::pin(
            async move { Storage::store_mapi_ipm_subtree_ost_id(self, account_id, ost_id).await },
        )
    }

    fn fetch_mapi_sync_changes<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        after_change_sequence: u64,
    ) -> StoreFuture<'a, MapiSyncChangeSet> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let cursor = sqlx::query(
                r#"
                SELECT
                    COALESCE(MAX(cursor), 0) AS current_change_sequence,
                    COALESCE(MAX(modseq), 1) AS current_modseq
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await?;
            let mut changes = MapiSyncChangeSet {
                current_change_sequence: cursor.get::<i64, _>("current_change_sequence") as u64,
                current_modseq: cursor.get::<i64, _>("current_modseq") as u64,
                ..Default::default()
            };
            let special_object_kind =
                mapi_special_object_kind_for_checkpoint_mailbox(checkpoint_kind, mailbox_id);

            let rows = sqlx::query(
                r#"
                SELECT object_kind, object_id, mailbox_id, change_kind, summary_json
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND cursor > $2
                  AND (account_id = $3 OR affected_principal_ids @> ARRAY[$3]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                  AND (
                    ($4 = 'hierarchy' AND object_kind IN ('mailbox', 'search_folder_definition'))
                    OR (
                        $4 IN ('content', 'read_state')
                        AND (
                            (
                                object_kind IN ('mailbox_message', 'attachment')
                                AND ($5::uuid IS NULL OR mailbox_id = $5 OR mailbox_id IS NULL)
                            )
                            OR ($5::uuid IS NULL AND object_kind IN (
                                'contact',
                                'calendar_event',
                                'task',
                                'note',
                                'journal_entry',
                                'conversation_action',
                                'navigation_shortcut',
                                'associated_config'
                            ))
                            OR object_kind = 'associated_config'
                            OR ($6::text IS NOT NULL AND object_kind = $6)
                        )
                    )
                  )
                  AND (
                    object_kind <> 'mailbox'
                    OR change_kind IN ('destroyed', 'expunged')
                    OR EXISTS (
                        SELECT 1
                        FROM mailboxes mailbox
                        WHERE mailbox.tenant_id = mail_change_log.tenant_id
                          AND mailbox.account_id = mail_change_log.account_id
                          AND mailbox.id = mail_change_log.object_id
                    )
                  )
                  AND (
                    object_kind <> 'associated_config'
                    OR change_kind IN ('destroyed', 'expunged')
                    OR (
                        EXISTS (
                            SELECT 1
                            FROM mapi_associated_config_messages config
                            WHERE config.tenant_id = mail_change_log.tenant_id
                              AND config.account_id = mail_change_log.account_id
                              AND config.id = mail_change_log.object_id
                        )
                        AND (summary_json ->> 'folderId') ~ '^[0-9]+$'
                        AND (
                            (summary_json ->> 'folderId')::bigint = ANY($7::bigint[])
                            OR EXISTS (
                                SELECT 1
                                FROM mapi_object_identities identity
                                WHERE identity.tenant_id = mail_change_log.tenant_id
                                  AND identity.account_id = mail_change_log.account_id
                                  AND identity.mapi_object_id = (summary_json ->> 'folderId')::bigint
                                  AND identity.object_kind IN ('mailbox', 'search_folder_definition')
                                  AND identity.deleted_at IS NULL
                            )
                        )
                    )
                  )
                  AND (
                    object_kind <> 'search_folder_definition'
                    OR change_kind IN ('destroyed', 'expunged')
                    OR EXISTS (
                        SELECT 1
                        FROM search_folders search_folder
                        WHERE search_folder.tenant_id = mail_change_log.tenant_id
                          AND search_folder.account_id = mail_change_log.account_id
                          AND search_folder.id = mail_change_log.object_id
                    )
                  )
                ORDER BY cursor ASC
                LIMIT 1000
                "#,
            )
            .bind(&tenant_id)
            .bind(after_change_sequence as i64)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(mailbox_id)
            .bind(special_object_kind)
            .bind(MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS.as_slice())
            .fetch_all(self.pool())
            .await?;

            for row in rows {
                let object_kind = row.get::<String, _>("object_kind");
                let change_kind = row.get::<String, _>("change_kind");
                let summary_json = row.get::<serde_json::Value, _>("summary_json");
                match object_kind.as_str() {
                    "mailbox" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            continue;
                        }
                        push_unique_uuid(&mut changes.changed_mailbox_ids, object_id);
                    }
                    "search_folder_definition" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            continue;
                        }
                        push_unique_uuid(&mut changes.changed_mailbox_ids, object_id);
                    }
                    "mailbox_message" | "attachment" => {
                        let Some(message_id) = summary_json
                            .get("messageId")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| Uuid::parse_str(value).ok())
                        else {
                            continue;
                        };
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_message_ids, message_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_message_ids, message_id);
                        }
                    }
                    "contact" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_contact_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_contact_ids, object_id);
                        }
                    }
                    "calendar_event" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_calendar_event_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_calendar_event_ids, object_id);
                        }
                    }
                    "task" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_task_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_task_ids, object_id);
                        }
                    }
                    "note" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_note_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_note_ids, object_id);
                        }
                    }
                    "journal_entry" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_journal_entry_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_journal_entry_ids, object_id);
                        }
                    }
                    "conversation_action" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(
                                &mut changes.deleted_conversation_action_ids,
                                object_id,
                            );
                        } else {
                            push_unique_uuid(
                                &mut changes.changed_conversation_action_ids,
                                object_id,
                            );
                        }
                    }
                    "navigation_shortcut" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(
                                &mut changes.deleted_navigation_shortcut_ids,
                                object_id,
                            );
                        } else {
                            push_unique_uuid(
                                &mut changes.changed_navigation_shortcut_ids,
                                object_id,
                            );
                        }
                    }
                    "associated_config" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        let Some(folder_id) = summary_json
                            .get("folderId")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| value.parse::<u64>().ok())
                        else {
                            continue;
                        };
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_associated_config_change(
                                &mut changes.deleted_associated_config_ids,
                                folder_id,
                                object_id,
                            );
                        } else {
                            push_unique_associated_config_change(
                                &mut changes.changed_associated_config_ids,
                                folder_id,
                                object_id,
                            );
                        }
                    }
                    _ => {}
                }
            }

            if checkpoint_kind == MapiCheckpointKind::Hierarchy {
                let mailbox_tombstones = sqlx::query(
                    r#"
                    SELECT DISTINCT identity.mapi_object_id
                    FROM tombstones tombstone
                    JOIN mapi_object_identities identity
                      ON identity.tenant_id = tombstone.tenant_id
                     AND identity.account_id = tombstone.account_id
                     AND identity.object_kind = 'mailbox'
                     AND identity.canonical_id = tombstone.object_id
                    WHERE tombstone.tenant_id = $1
                      AND tombstone.account_id = $2
                      AND tombstone.object_kind = 'mailbox'
                      AND tombstone.change_cursor > $3
                      AND (tombstone.retained_until IS NULL OR tombstone.retained_until > NOW())
                    ORDER BY identity.mapi_object_id
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .fetch_all(self.pool())
                .await?;
                for row in mailbox_tombstones {
                    let object_id = row.get::<i64, _>("mapi_object_id");
                    if object_id > 0 {
                        changes.deleted_mailbox_object_ids.push(object_id as u64);
                    }
                }
                let search_folder_tombstones = sqlx::query(
                    r#"
                    SELECT DISTINCT identity.mapi_object_id
                    FROM tombstones tombstone
                    JOIN mapi_object_identities identity
                      ON identity.tenant_id = tombstone.tenant_id
                     AND identity.account_id = tombstone.account_id
                     AND identity.object_kind = 'search_folder_definition'
                     AND identity.canonical_id = tombstone.object_id
                    WHERE tombstone.tenant_id = $1
                      AND tombstone.account_id = $2
                      AND tombstone.object_kind = 'search_folder_definition'
                      AND tombstone.change_cursor > $3
                      AND (tombstone.retained_until IS NULL OR tombstone.retained_until > NOW())
                    ORDER BY identity.mapi_object_id
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .fetch_all(self.pool())
                .await?;
                for row in search_folder_tombstones {
                    let object_id = row.get::<i64, _>("mapi_object_id");
                    if object_id > 0 {
                        changes
                            .deleted_search_folder_object_ids
                            .push(object_id as u64);
                    }
                }
            }

            if checkpoint_kind != MapiCheckpointKind::Hierarchy {
                let tombstones = sqlx::query(
                    r#"
                    SELECT message_id
                    FROM tombstones
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = 'mailbox_message'
                      AND change_cursor > $3
                      AND ($4::uuid IS NULL OR mailbox_id = $4)
                      AND message_id IS NOT NULL
                      AND (retained_until IS NULL OR retained_until > NOW())
                    ORDER BY change_cursor ASC
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .bind(mailbox_id)
                .fetch_all(self.pool())
                .await?;
                for row in tombstones {
                    push_unique_uuid(&mut changes.deleted_message_ids, row.get("message_id"));
                }
                let collaboration_tombstones = sqlx::query(
                    r#"
                    SELECT object_kind, object_id
                    FROM tombstones
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind IN (
                          'contact',
                          'calendar_event',
                          'task',
                          'note',
                          'journal_entry',
                          'conversation_action'
                      )
                      AND change_cursor > $3
                      AND ($4::uuid IS NULL OR object_kind = $5)
                      AND (retained_until IS NULL OR retained_until > NOW())
                    ORDER BY change_cursor ASC
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .bind(mailbox_id)
                .bind(special_object_kind)
                .fetch_all(self.pool())
                .await?;
                for row in collaboration_tombstones {
                    match row.get::<String, _>("object_kind").as_str() {
                        "contact" => {
                            push_unique_uuid(&mut changes.deleted_contact_ids, row.get("object_id"))
                        }
                        "calendar_event" => push_unique_uuid(
                            &mut changes.deleted_calendar_event_ids,
                            row.get("object_id"),
                        ),
                        "task" => {
                            push_unique_uuid(&mut changes.deleted_task_ids, row.get("object_id"))
                        }
                        "note" => {
                            push_unique_uuid(&mut changes.deleted_note_ids, row.get("object_id"))
                        }
                        "journal_entry" => push_unique_uuid(
                            &mut changes.deleted_journal_entry_ids,
                            row.get("object_id"),
                        ),
                        "conversation_action" => push_unique_uuid(
                            &mut changes.deleted_conversation_action_ids,
                            row.get("object_id"),
                        ),
                        _ => {}
                    }
                }
            }

            Ok(changes)
        })
    }

    fn fetch_mapi_folder_permissions<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<MapiFolderPermission>> {
        Box::pin(async move {
            let row = sqlx::query(
                r#"
                SELECT tenant_id, primary_email, display_name
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let principal = lpe_mail_auth::AccountPrincipal {
                tenant_id: row.get("tenant_id"),
                account_id,
                email: row.get("primary_email"),
                display_name: row.get("display_name"),
                quota_mb: None,
                quota_used_octets: None,
            };
            let mut permissions = mailbox_ids
                .iter()
                .copied()
                .map(|mailbox_id| owner_permission(mailbox_id, &principal))
                .collect::<Vec<_>>();
            if mailbox_ids.is_empty() {
                return Ok(permissions);
            }

            let rows = sqlx::query(
                r#"
                SELECT
                    g.mailbox_id,
                    g.grantee_account_id,
                    grantee.display_name,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM mailbox_delegation_grants g
                JOIN accounts grantee
                  ON grantee.tenant_id = g.tenant_id
                 AND grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.mailbox_id = ANY($2)
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(principal.tenant_id)
            .bind(mailbox_ids)
            .fetch_all(self.pool())
            .await?;

            permissions.extend(rows.into_iter().map(|row| MapiFolderPermission {
                mailbox_id: row.get("mailbox_id"),
                member_account_id: Some(row.get("grantee_account_id")),
                member_name: row.get("display_name"),
                rights: rights_from_grant(
                    row.get("may_read"),
                    row.get("may_write"),
                    row.get("may_delete"),
                    row.get("may_share"),
                ),
            }));
            Ok(permissions)
        })
    }

    fn set_mapi_folder_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        mailbox_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.set_mailbox_folder_delegation_grant(
                MailboxFolderDelegationGrantInput {
                    owner_account_id,
                    mailbox_id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                },
                audit,
            )
            .await
        })
    }

    fn set_mapi_calendar_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if !may_read {
                return self
                    .delete_collaboration_grant(
                        owner_account_id,
                        CollaborationResourceKind::Calendar,
                        grantee_account_id,
                        audit,
                    )
                    .await;
            }
            let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
            let grantee_email = sqlx::query_scalar::<_, String>(
                r#"
                SELECT primary_email
                FROM accounts
                WHERE tenant_id = $1
                  AND id = $2
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(grantee_account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("calendar permission grantee account not found"))?;
            self.upsert_collaboration_grant(
                CollaborationGrantInput {
                    kind: CollaborationResourceKind::Calendar,
                    owner_account_id,
                    grantee_email,
                    calendar_id: None,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                },
                audit,
            )
            .await
            .map(|_| ())
        })
    }

    fn set_mapi_calendar_collection_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        calendar_collection_id: &'a str,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.set_calendar_collection_grant(
                owner_account_id,
                calendar_collection_id,
                grantee_account_id,
                may_read,
                may_write,
                may_delete,
                may_share,
                audit,
            )
            .await
        })
    }

    };
}
