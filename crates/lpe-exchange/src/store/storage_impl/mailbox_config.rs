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
                       flags, save_stamp, section, ordinal, group_header_id, group_name,
                       calendar_color, address_book_entry_id,
                       address_book_store_entry_id, client_id, ro_group_type
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

    #[cfg(test)]
    fn upsert_mapi_navigation_shortcut<'a>(
        &'a self,
        input: UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutRecord> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let shortcut = upsert_mapi_navigation_shortcut_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(shortcut)
        })
    }

    fn commit_mapi_navigation_shortcut_create<'a>(
        &'a self,
        input: CommitMapiNavigationShortcutCreateInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutCommit> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.shortcut.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let committed =
                commit_mapi_navigation_shortcut_create_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(committed)
        })
    }

    fn commit_mapi_navigation_shortcut_update<'a>(
        &'a self,
        input: UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutCommit> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let committed =
                commit_mapi_navigation_shortcut_update_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(committed)
        })
    }

    fn commit_mapi_navigation_shortcut_import<'a>(
        &'a self,
        input: CommitMapiNavigationShortcutImportInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutImportCommit> {
        Box::pin(async move {
            let tenant_id =
                mapi_tenant_id_for_account(self, input.shortcut.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let identity_commit = commit_mapi_imported_fai_identity_in_tx(
                &mut tx,
                tenant_id,
                input.shortcut.account_id,
                MapiIdentityObjectKind::NavigationShortcut,
                crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                input.shortcut.id,
                &input.identity,
                input.fail_on_conflict,
            )
            .await?;
            let mut shortcut_input = input.shortcut;
            shortcut_input.id = Some(identity_commit.canonical_id);
            let shortcut = if identity_commit.apply_imported_content {
                upsert_mapi_navigation_shortcut_in_tx(&mut tx, tenant_id, shortcut_input).await?
            } else {
                let shortcut = fetch_mapi_navigation_shortcut_in_tx(
                    &mut tx,
                    tenant_id,
                    shortcut_input.account_id,
                    identity_commit.canonical_id,
                )
                .await?;
                if identity_commit.disposition.changes_server_replica() {
                    insert_mapi_navigation_shortcut_change(
                        &mut tx,
                        tenant_id,
                        shortcut_input.account_id,
                        identity_commit.canonical_id,
                        "updated",
                    )
                    .await?;
                }
                shortcut
            };
            tx.commit().await?;
            Ok(MapiNavigationShortcutImportCommit {
                shortcut,
                identity: identity_commit.identity,
                disposition: identity_commit.disposition,
            })
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
            delete_mapi_navigation_shortcut_in_tx(
                &mut tx,
                tenant_id,
                account_id,
                shortcut_id,
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn preflight_unknown_mapi_navigation_shortcut_deletes<'a>(
        &'a self,
        account_id: Uuid,
        folder_id: u64,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            for source_key in source_keys {
                preflight_unknown_mapi_navigation_shortcut_delete_in_tx(
                    &mut tx,
                    tenant_id,
                    account_id,
                    folder_id,
                    source_key,
                )
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn tombstone_unknown_mapi_navigation_shortcut<'a>(
        &'a self,
        account_id: Uuid,
        folder_id: u64,
        source_key: &'a [u8],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            tombstone_unknown_mapi_navigation_shortcut_in_tx(
                &mut tx,
                tenant_id,
                account_id,
                folder_id,
                source_key,
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
                SELECT config.id, config.account_id, config.folder_id,
                       config.message_class, config.subject, config.properties_json,
                       to_char(
                           config.created_at AT TIME ZONE 'UTC',
                           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                       ) AS created_at,
                       to_char(
                           config.updated_at AT TIME ZONE 'UTC',
                           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                       ) AS updated_at,
                       account.display_name AS last_modifier_name
                FROM mapi_associated_config_messages config
                JOIN accounts account
                  ON account.tenant_id = config.tenant_id
                 AND account.id = config.account_id
                WHERE config.tenant_id = $1 AND config.account_id = $2
                ORDER BY config.folder_id, config.message_class, config.subject,
                         config.updated_at DESC, config.id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter().map(mapi_associated_config_from_row).collect()
        })
    }

    fn upsert_mapi_associated_config<'a>(
        &'a self,
        input: UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigRecord> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let mut input = input;
            input.id = Some(input.id.unwrap_or_else(Uuid::new_v4));
            let saved = upsert_mapi_associated_config_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(saved)
        })
    }

    fn commit_mapi_associated_config_create<'a>(
        &'a self,
        input: UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigCommit> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let committed =
                commit_mapi_associated_config_create_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(committed)
        })
    }

    fn commit_mapi_associated_config_import<'a>(
        &'a self,
        input: CommitMapiAssociatedConfigImportInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigImportCommit> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.config.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let identity_commit = commit_mapi_imported_fai_identity_in_tx(
                &mut tx,
                tenant_id,
                input.config.account_id,
                MapiIdentityObjectKind::AssociatedConfig,
                input.config.folder_id,
                input.config.id,
                &input.identity,
                input.fail_on_conflict,
            )
            .await?;
            let mut config_input = input.config;
            config_input.id = Some(identity_commit.canonical_id);
            let config = if identity_commit.apply_imported_content {
                upsert_mapi_associated_config_in_tx(&mut tx, tenant_id, config_input).await?
            } else {
                let config = fetch_mapi_associated_config_in_tx(
                    &mut tx,
                    tenant_id,
                    config_input.account_id,
                    identity_commit.canonical_id,
                )
                .await?;
                if identity_commit.disposition.changes_server_replica() {
                    insert_mapi_associated_config_change(
                        &mut tx,
                        tenant_id,
                        config_input.account_id,
                        identity_commit.canonical_id,
                        "updated",
                        config.folder_id,
                    )
                    .await?;
                }
                config
            };
            tx.commit().await?;
            Ok(MapiAssociatedConfigImportCommit {
                config,
                identity: identity_commit.identity,
                disposition: identity_commit.disposition,
            })
        })
    }

    fn commit_mapi_associated_config_update<'a>(
        &'a self,
        input: UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigCommit> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let mut tx = self.pool().begin().await?;
            let committed =
                commit_mapi_associated_config_update_in_tx(&mut tx, tenant_id, input).await?;
            tx.commit().await?;
            Ok(committed)
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
            delete_mapi_associated_config_in_tx(&mut tx, tenant_id, account_id, config_id).await?;
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
