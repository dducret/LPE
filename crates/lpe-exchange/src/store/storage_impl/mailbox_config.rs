macro_rules! store_impl_mailbox_config {
    () => {
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
    }

    fn ensure_jmap_system_mailboxes<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.ensure_imap_mailboxes(account_id).await })
    }

    fn fetch_search_folders<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SearchFolderDefinition>> {
        Box::pin(async move { self.fetch_search_folders(account_id).await })
    }

    fn upsert_search_folder<'a>(
        &'a self,
        input: UpsertSearchFolderInput,
    ) -> StoreFuture<'a, SearchFolderDefinition> {
        Box::pin(async move { self.upsert_search_folder(input).await })
    }

    fn delete_search_folder<'a>(
        &'a self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_search_folder(account_id, search_folder_id)
                .await
        })
    }

    fn fetch_conversation_actions<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ConversationAction>> {
        Box::pin(async move { self.fetch_conversation_actions(account_id).await })
    }

    fn fetch_mapi_navigation_shortcuts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiNavigationShortcutRecord>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT id, account_id, subject, target_folder_id, shortcut_type,
                       flags, save_stamp, section, ordinal, group_header_id, group_name
                FROM mapi_navigation_shortcuts
                WHERE tenant_id = $1 AND account_id = $2
                ORDER BY section, ordinal, subject, updated_at DESC, id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_navigation_shortcut_from_row)
                .collect()
        })
    }

    fn upsert_mapi_navigation_shortcut<'a>(
        &'a self,
        input: UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutRecord> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let default_group_header_id = crate::mapi::properties::default_wlink_group_uuid();
            let mut tx = self.pool().begin().await?;
            let id = match input.id {
                Some(id) => id,
                None => {
                    let target_folder_id = input.target_folder_id.map(|value| value as i64);
                    if target_folder_id.is_some() {
                        sqlx::query_scalar::<_, Uuid>(
                            r#"
                            SELECT id
                            FROM mapi_navigation_shortcuts
                            WHERE tenant_id = $1
                              AND account_id = $2
                              AND target_folder_id IS NOT DISTINCT FROM $3
                              AND shortcut_type = $4
                              AND section = $5
                            ORDER BY updated_at DESC, id
                            LIMIT 1
                            "#,
                        )
                        .bind(tenant_id)
                        .bind(input.account_id)
                        .bind(target_folder_id)
                        .bind(input.shortcut_type as i64)
                        .bind(input.section as i64)
                        .fetch_optional(&mut *tx)
                        .await?
                    } else {
                        sqlx::query_scalar::<_, Uuid>(
                            r#"
                            SELECT id
                            FROM mapi_navigation_shortcuts
                            WHERE tenant_id = $1
                              AND account_id = $2
                              AND subject = $3
                              AND target_folder_id IS NOT DISTINCT FROM $4
                              AND shortcut_type = $5
                              AND section = $6
                              AND COALESCE(group_header_id, $9) = COALESCE($7, $9)
                              AND group_name = $8
                            ORDER BY updated_at DESC, id
                            LIMIT 1
                            "#,
                        )
                        .bind(tenant_id)
                        .bind(input.account_id)
                        .bind(&input.subject)
                        .bind(target_folder_id)
                        .bind(input.shortcut_type as i64)
                        .bind(input.section as i64)
                        .bind(input.group_header_id)
                        .bind(&input.group_name)
                        .bind(default_group_header_id)
                        .fetch_optional(&mut *tx)
                        .await?
                    }
                    .unwrap_or_else(Uuid::new_v4)
                }
            };
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
            .fetch_one(&mut *tx)
            .await?;
            let row = sqlx::query(
                r#"
                INSERT INTO mapi_navigation_shortcuts (
                    tenant_id, id, account_id, subject, target_folder_id,
                    shortcut_type, flags, save_stamp, section, ordinal, group_header_id, group_name
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
                    updated_at = NOW()
                RETURNING id, account_id, subject, target_folder_id, shortcut_type,
                          flags, save_stamp, section, ordinal, group_header_id, group_name
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
            .bind(input.ordinal as i64)
            .bind(input.group_header_id)
            .bind(input.group_name)
            .fetch_one(&mut *tx)
            .await?;

            let superseded_ids = sqlx::query_scalar::<_, Uuid>(
                r#"
                DELETE FROM mapi_navigation_shortcuts
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND id <> $3
                  AND (
                    (
                      target_folder_id IS NOT NULL
                      AND target_folder_id IS NOT DISTINCT FROM $5
                      AND shortcut_type = $6
                      AND section = $7
                    )
                    OR (
                      target_folder_id IS NULL
                      AND subject = $4
                      AND target_folder_id IS NOT DISTINCT FROM $5
                      AND shortcut_type = $6
                      AND section = $7
                      AND COALESCE(group_header_id, $10) = COALESCE($8, $10)
                      AND group_name = $9
                    )
                  )
                RETURNING id
                "#,
            )
            .bind(tenant_id)
            .bind(input.account_id)
            .bind(id)
            .bind(&row.try_get::<String, _>("subject")?)
            .bind(row.try_get::<Option<i64>, _>("target_folder_id")?)
            .bind(row.try_get::<i64, _>("shortcut_type")?)
            .bind(row.try_get::<i64, _>("section")?)
            .bind(row.try_get::<Option<Uuid>, _>("group_header_id")?)
            .bind(&row.try_get::<String, _>("group_name")?)
            .bind(default_group_header_id)
            .fetch_all(&mut *tx)
            .await?;
            for superseded_id in superseded_ids {
                insert_mapi_navigation_shortcut_change(
                    &mut tx,
                    tenant_id,
                    input.account_id,
                    superseded_id,
                    "destroyed",
                )
                .await?;
            }
            insert_mapi_navigation_shortcut_change(
                &mut tx,
                tenant_id,
                input.account_id,
                id,
                if existed { "updated" } else { "created" },
            )
            .await?;
            tx.commit().await?;

            mapi_navigation_shortcut_from_row(row)
        })
    }

    fn delete_mapi_navigation_shortcut<'a>(
        &'a self,
        account_id: Uuid,
        shortcut_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            let deleted = sqlx::query_scalar::<_, Uuid>(
                r#"
                DELETE FROM mapi_navigation_shortcuts
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                RETURNING id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(shortcut_id)
            .fetch_optional(&mut *tx)
            .await?;
            if deleted.is_none() {
                anyhow::bail!("MAPI navigation shortcut not found");
            }
            insert_mapi_navigation_shortcut_change(
                &mut tx,
                tenant_id,
                account_id,
                shortcut_id,
                "destroyed",
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn fetch_mapi_associated_configs<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiAssociatedConfigRecord>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT id, account_id, folder_id, message_class, subject, properties_json,
                       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM mapi_associated_config_messages
                WHERE tenant_id = $1 AND account_id = $2
                ORDER BY folder_id, message_class, subject, updated_at DESC, id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;

            let mut seen = HashSet::new();
            rows.into_iter()
                .map(mapi_associated_config_from_row)
                .filter_map(|result| match result {
                    Ok(config) => {
                        if seen.insert((
                            config.folder_id,
                            config.message_class.clone(),
                            config.subject.clone(),
                        )) {
                            Some(Ok(config))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err)),
                })
                .collect()
        })
    }

    fn upsert_mapi_associated_config<'a>(
        &'a self,
        input: UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigRecord> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let message_class = input.message_class;
            let subject = input.subject;
            let properties_json = input.properties_json;
            let explicit_exists = if let Some(id) = input.id {
                sqlx::query_scalar::<_, bool>(
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
                .fetch_one(&mut *tx)
                .await?
            } else {
                false
            };
            let logical_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM mapi_associated_config_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND folder_id = $3
                  AND message_class = $4
                  AND subject = $5
                ORDER BY updated_at DESC, id
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(input.account_id)
            .bind(input.folder_id as i64)
            .bind(&message_class)
            .bind(&subject)
            .fetch_optional(&mut *tx)
            .await?;
            let id = match input.id {
                Some(id) => {
                    if explicit_exists {
                        id
                    } else {
                        logical_id.unwrap_or(id)
                    }
                }
                None => logical_id.unwrap_or_else(Uuid::new_v4),
            };
            let has_subject_logical_index = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_index index_info
                    JOIN pg_class index_class
                      ON index_class.oid = index_info.indexrelid
                    WHERE index_info.indrelid = 'mapi_associated_config_messages'::regclass
                      AND index_class.relname = 'mapi_associated_config_messages_logical_idx'
                      AND pg_get_indexdef(index_class.oid) LIKE '%subject%'
                )
                "#,
            )
            .fetch_one(&mut *tx)
            .await?;
            let existed = explicit_exists || logical_id.is_some();
            let row = if explicit_exists {
                sqlx::query(
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
                              to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                    "#,
                )
                .bind(tenant_id)
                .bind(id)
                .bind(input.account_id)
                .bind(input.folder_id as i64)
                .bind(&message_class)
                .bind(&subject)
                .bind(&properties_json)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MAPI associated config message not found"))?
            } else if has_subject_logical_index {
                sqlx::query(
                    r#"
                    INSERT INTO mapi_associated_config_messages (
                        tenant_id, id, account_id, folder_id, message_class, subject, properties_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (tenant_id, account_id, folder_id, message_class, subject)
                    DO UPDATE SET
                        properties_json = EXCLUDED.properties_json,
                        updated_at = NOW()
                    RETURNING id, account_id, folder_id, message_class, subject, properties_json,
                              to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                    "#,
                )
                .bind(tenant_id)
                .bind(id)
                .bind(input.account_id)
                .bind(input.folder_id as i64)
                .bind(&message_class)
                .bind(&subject)
                .bind(&properties_json)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MAPI associated config message not found"))?
            } else {
                sqlx::query(
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
                              to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                    "#,
                )
                .bind(tenant_id)
                .bind(id)
                .bind(input.account_id)
                .bind(input.folder_id as i64)
                .bind(&message_class)
                .bind(&subject)
                .bind(&properties_json)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MAPI associated config message not found"))?
            };
            let saved = mapi_associated_config_from_row(row)?;
            let existed = existed || saved.id != id;
            sqlx::query(
                r#"
                DELETE FROM mapi_associated_config_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND id <> $3
                  AND folder_id = $4
                  AND message_class = $5
                  AND subject = $6
                "#,
            )
            .bind(tenant_id)
            .bind(input.account_id)
            .bind(saved.id)
            .bind(saved.folder_id as i64)
            .bind(&saved.message_class)
            .bind(&saved.subject)
            .execute(&mut *tx)
            .await?;
            insert_mapi_associated_config_change(
                &mut tx,
                tenant_id,
                input.account_id,
                saved.id,
                if existed { "updated" } else { "created" },
                saved.folder_id,
            )
            .await?;
            tx.commit().await?;

            Ok(saved)
        })
    }

    fn delete_mapi_associated_config<'a>(
        &'a self,
        account_id: Uuid,
        config_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            let deleted = sqlx::query_scalar::<_, i64>(
                r#"
                DELETE FROM mapi_associated_config_messages
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                RETURNING folder_id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(config_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(folder_id) = deleted else {
                anyhow::bail!("MAPI associated config message not found");
            };
            insert_mapi_associated_config_change(
                &mut tx,
                tenant_id,
                account_id,
                config_id,
                "destroyed",
                folder_id as u64,
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn upsert_conversation_action<'a>(
        &'a self,
        input: UpsertConversationActionInput,
    ) -> StoreFuture<'a, ConversationAction> {
        Box::pin(async move { self.upsert_conversation_action(input).await })
    }

    fn delete_conversation_action<'a>(
        &'a self,
        account_id: Uuid,
        conversation_action_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_conversation_action(account_id, conversation_action_id)
                .await
        })
    }

    fn query_client_reminders<'a>(
        &'a self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> StoreFuture<'a, Vec<ClientReminder>> {
        Box::pin(async move { self.query_client_reminders(account_id, query).await })
    }

    fn dismiss_reminder_occurrence<'a>(
        &'a self,
        account_id: Uuid,
        source_type: &'a str,
        source_id: Uuid,
        occurrence_start_at: Option<&'a str>,
        dismissed_at: &'a str,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.dismiss_reminder_occurrence(
                account_id,
                source_type,
                source_id,
                occurrence_start_at.unwrap_or_default(),
                dismissed_at,
            )
            .await
        })
    }

    };
}
