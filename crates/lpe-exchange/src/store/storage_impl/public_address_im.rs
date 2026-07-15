macro_rules! store_impl_public_address_im {
    () => {
    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(cursor)
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await
            .map_err(Into::into)
        })
    }

    fn fetch_public_folder_trees<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderTree>> {
        Box::pin(
            async move { Storage::fetch_public_folder_trees(self, principal_account_id).await },
        )
    }

    fn fetch_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, PublicFolder> {
        Box::pin(async move {
            Storage::fetch_public_folder(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_children<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolder>> {
        Box::pin(async move {
            Storage::fetch_public_folder_children(self, principal_account_id, folder_id).await
        })
    }

    fn create_public_folder_child<'a>(
        &'a self,
        input: CreatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder> {
        Box::pin(async move { Storage::create_public_folder_child(self, input, audit).await })
    }

    fn update_public_folder<'a>(
        &'a self,
        input: UpdatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder> {
        Box::pin(async move { Storage::update_public_folder(self, input, audit).await })
    }

    fn delete_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder(self, principal_account_id, folder_id, audit).await
        })
    }

    fn fetch_public_folder_items<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        Box::pin(async move {
            Storage::fetch_public_folder_items(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_items_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        item_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        Box::pin(async move {
            Storage::fetch_public_folder_items_by_ids(self, principal_account_id, item_ids).await
        })
    }

    fn fetch_public_folder_permissions<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPermission>> {
        Box::pin(async move {
            Storage::fetch_public_folder_permissions(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_replicas<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderReplica>> {
        Box::pin(async move {
            Storage::fetch_public_folder_replicas(self, principal_account_id, folder_id).await
        })
    }

    fn upsert_public_folder_permission<'a>(
        &'a self,
        input: PublicFolderPermissionInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderPermission> {
        Box::pin(async move { Storage::upsert_public_folder_permission(self, input, audit).await })
    }

    fn delete_public_folder_permission<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder_permission(
                self,
                principal_account_id,
                folder_id,
                grantee_account_id,
                audit,
            )
            .await
        })
    }

    fn upsert_public_folder_item<'a>(
        &'a self,
        input: UpsertPublicFolderItemInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderItem> {
        Box::pin(async move { Storage::upsert_public_folder_item(self, input, audit).await })
    }

    fn delete_public_folder_item<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder_item(
                self,
                principal_account_id,
                folder_id,
                item_id,
                audit,
            )
            .await
        })
    }

    fn fetch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        Box::pin(async move {
            Storage::fetch_public_folder_per_user_state(self, principal_account_id, folder_id).await
        })
    }

    fn patch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        patches: &'a [PublicFolderPerUserStatePatch],
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        Box::pin(async move {
            Storage::patch_public_folder_per_user_state(
                self,
                principal_account_id,
                folder_id,
                patches,
            )
            .await
        })
    }

    fn poll_mapi_notifications<'a>(
        &'a self,
        account_id: Uuid,
        after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let current_cursor = sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(cursor)
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await?;
            let rows = sqlx::query(
                r#"
                SELECT
                    log.cursor,
                    log.object_kind,
                    log.object_id,
                    log.account_id AS owner_account_id,
                    $3::uuid AS notification_account_id,
                    log.mailbox_id,
                    NULLIF(log.summary_json->>'messageId', '')::uuid AS message_id,
                    log.change_kind,
                    log.modseq,
                    log.summary_json,
                    scope_box.display_name AS scope_display_name,
                    scope_box.role AS scope_role,
                    scope_box.total_messages AS scope_total_messages,
                    scope_box.unread_messages AS scope_unread_messages,
                    scope_parent_box.role AS scope_parent_role,
                    object_box.display_name AS object_display_name,
                    object_box.role AS object_role,
                    object_box.parent_mailbox_id AS object_parent_id,
                    object_box.total_messages AS object_total_messages,
                    object_box.unread_messages AS object_unread_messages,
                    parent_box.display_name AS parent_display_name,
                    parent_box.role AS parent_role,
                    source_box.display_name AS source_display_name,
                    message.normalized_subject AS message_subject,
                    event_calendar.id AS calendar_id,
                    event_calendar.role AS calendar_role,
                    NULLIF(log.summary_json->>'oldCollectionId', '')::uuid AS old_calendar_id,
                    COALESCE(
                        old_event_calendar.role,
                        NULLIF(log.summary_json->>'oldCollectionRole', '')
                    ) AS old_calendar_role,
                    calendar_event.id AS live_calendar_event_id,
                    calendar_event.title AS calendar_event_subject,
                    scope_identity.mapi_object_id AS scope_mapi_object_id,
                    scope_parent_identity.mapi_object_id AS scope_parent_mapi_object_id,
                    object_identity.mapi_object_id AS object_mapi_object_id,
                    parent_identity.mapi_object_id AS parent_mapi_object_id,
                    message_identity.mapi_object_id AS message_mapi_object_id,
                    source_identity.mapi_object_id AS source_mapi_object_id,
                    calendar_event_identity.mapi_object_id AS calendar_event_mapi_object_id
                FROM mail_change_log log
                LEFT JOIN mailboxes scope_box
                  ON scope_box.tenant_id = log.tenant_id
                 AND scope_box.account_id = log.account_id
                 AND scope_box.id = log.mailbox_id
                LEFT JOIN mailboxes object_box
                  ON object_box.tenant_id = log.tenant_id
                 AND object_box.account_id = log.account_id
                 AND object_box.id = log.object_id
                 AND log.object_kind = 'mailbox'
                LEFT JOIN mailboxes scope_parent_box
                  ON scope_parent_box.tenant_id = scope_box.tenant_id
                 AND scope_parent_box.account_id = scope_box.account_id
                 AND scope_parent_box.id = scope_box.parent_mailbox_id
                LEFT JOIN mailboxes parent_box
                  ON parent_box.tenant_id = object_box.tenant_id
                 AND parent_box.account_id = object_box.account_id
                 AND parent_box.id = object_box.parent_mailbox_id
                LEFT JOIN mailboxes source_box
                  ON source_box.tenant_id = log.tenant_id
                 AND source_box.account_id = log.account_id
                 AND source_box.id = (log.summary_json->>'sourceMailboxId')::uuid
                LEFT JOIN messages message
                  ON message.tenant_id = log.tenant_id
                 AND message.id = (log.summary_json->>'messageId')::uuid
                LEFT JOIN tombstones calendar_tombstone
                  ON calendar_tombstone.tenant_id = log.tenant_id
                 AND calendar_tombstone.change_cursor = log.cursor
                 AND calendar_tombstone.object_kind = 'calendar_event'
                 AND calendar_tombstone.object_id = log.object_id
                 AND log.object_kind = 'calendar_event'
                LEFT JOIN calendar_events calendar_event
                  ON calendar_event.tenant_id = log.tenant_id
                 AND calendar_event.owner_account_id = log.account_id
                 AND calendar_event.id = log.object_id
                 AND log.object_kind = 'calendar_event'
                LEFT JOIN calendars event_calendar
                  ON event_calendar.tenant_id = log.tenant_id
                 AND event_calendar.owner_account_id = log.account_id
                 AND event_calendar.id = COALESCE(
                     NULLIF(log.summary_json->>'collectionId', '')::uuid,
                     calendar_tombstone.collection_id,
                     calendar_event.calendar_id
                 )
                 AND log.object_kind = 'calendar_event'
                LEFT JOIN calendars old_event_calendar
                  ON old_event_calendar.tenant_id = log.tenant_id
                 AND old_event_calendar.owner_account_id = log.account_id
                 AND old_event_calendar.id =
                     NULLIF(log.summary_json->>'oldCollectionId', '')::uuid
                 AND log.object_kind = 'calendar_event'
                LEFT JOIN mapi_object_identities scope_identity
                  ON scope_identity.tenant_id = log.tenant_id
                 AND scope_identity.account_id = log.account_id
                 AND scope_identity.object_kind = 'mailbox'
                 AND scope_identity.canonical_id = log.mailbox_id
                 AND scope_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities object_identity
                  ON object_identity.tenant_id = log.tenant_id
                 AND object_identity.account_id = log.account_id
                 AND object_identity.object_kind = 'mailbox'
                 AND object_identity.canonical_id = log.object_id
                 AND object_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities scope_parent_identity
                  ON scope_parent_identity.tenant_id = log.tenant_id
                 AND scope_parent_identity.account_id = log.account_id
                 AND scope_parent_identity.object_kind = 'mailbox'
                 AND scope_parent_identity.canonical_id = scope_box.parent_mailbox_id
                 AND scope_parent_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities parent_identity
                  ON parent_identity.tenant_id = log.tenant_id
                 AND parent_identity.account_id = log.account_id
                 AND parent_identity.object_kind = 'mailbox'
                 AND parent_identity.canonical_id = object_box.parent_mailbox_id
                 AND parent_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities message_identity
                  ON message_identity.tenant_id = log.tenant_id
                 AND message_identity.account_id = log.account_id
                 AND message_identity.object_kind = 'message'
                 AND message_identity.canonical_id = (log.summary_json->>'messageId')::uuid
                 AND message_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities source_identity
                  ON source_identity.tenant_id = log.tenant_id
                 AND source_identity.account_id = log.account_id
                 AND source_identity.object_kind = 'mailbox'
                 AND source_identity.canonical_id = (log.summary_json->>'sourceMailboxId')::uuid
                 AND source_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities calendar_event_identity
                  ON calendar_event_identity.tenant_id = log.tenant_id
                 AND calendar_event_identity.account_id = $3
                 AND calendar_event_identity.object_kind = 'calendar_event'
                 AND calendar_event_identity.canonical_id = log.object_id
                WHERE log.tenant_id = $1
                  AND log.cursor > $2
                  AND (log.account_id = $3 OR log.affected_principal_ids @> ARRAY[$3]::uuid[])
                  AND (log.retained_until IS NULL OR log.retained_until > NOW())
                  AND log.object_kind IN (
                      'mailbox', 'mailbox_message', 'attachment', 'calendar_event'
                  )
                ORDER BY log.cursor ASC
                LIMIT 101
                "#,
            )
            .bind(tenant_id)
            .bind(after_cursor)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;
            let mut notification_identity_requests = Vec::new();
            for row in &rows {
                if row.get::<String, _>("object_kind") != "calendar_event" {
                    continue;
                }
                if row
                    .try_get::<Option<i64>, _>("calendar_event_mapi_object_id")
                    .ok()
                    .flatten()
                    .is_none()
                {
                    if let Some(canonical_id) = row
                        .try_get::<Option<Uuid>, _>("live_calendar_event_id")
                        .ok()
                        .flatten()
                        .filter(|canonical_id| {
                            !notification_identity_requests.iter().any(
                                |request: &MapiIdentityRequest| {
                                    request.object_kind == MapiIdentityObjectKind::CalendarEvent
                                        && request.canonical_id == *canonical_id
                                },
                            )
                        })
                    {
                        notification_identity_requests.push(MapiIdentityRequest {
                            object_kind: MapiIdentityObjectKind::CalendarEvent,
                            canonical_id,
                            reserved_global_counter: None,
                            source_key: None,
                        });
                    }
                }
                for canonical_id in
                    mapi_calendar_notification_folder_identity_ids_from_row(row)
                {
                    if notification_identity_requests.iter().any(
                        |request: &MapiIdentityRequest| {
                            request.object_kind == MapiIdentityObjectKind::Mailbox
                                && request.canonical_id == canonical_id
                        },
                    ) {
                        continue;
                    }
                    notification_identity_requests.push(MapiIdentityRequest {
                        object_kind: MapiIdentityObjectKind::Mailbox,
                        canonical_id,
                        reserved_global_counter: None,
                        source_key: None,
                    });
                }
            }
            let mut calendar_folder_ids = std::collections::HashMap::new();
            let mut calendar_event_ids = std::collections::HashMap::new();
            if !notification_identity_requests.is_empty() {
                let identities = ExchangeStore::fetch_or_allocate_mapi_identities(
                    self,
                    account_id,
                    &notification_identity_requests,
                )
                .await?;
                for (request, identity) in
                    notification_identity_requests.iter().zip(identities)
                {
                    if request.object_kind == MapiIdentityObjectKind::Mailbox {
                        calendar_folder_ids.insert(identity.canonical_id, identity.object_id);
                    } else if request.object_kind == MapiIdentityObjectKind::CalendarEvent {
                        calendar_event_ids.insert(identity.canonical_id, identity.object_id);
                    }
                    crate::mapi::identity::remember_mapi_identity_with_source_key(
                        identity.canonical_id,
                        identity.object_id,
                        Some(identity.source_key),
                    );
                }
            }
            let truncated = rows.len() > 100;
            let mut cursor = None;
            let mut events = Vec::new();
            for row in rows.into_iter().take(100) {
                cursor = Some(row.get("cursor"));
                if let Some(event) = mapi_notification_event_from_change_row(
                    row,
                    &calendar_folder_ids,
                    &calendar_event_ids,
                ) {
                    events.push(event);
                }
            }
            let cursor = cursor.or(current_cursor);
            Ok(MapiNotificationPoll {
                event_pending: truncated || !events.is_empty(),
                cursor,
                events,
            })
        })
    }

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE tenant_id = $1
                  AND id = $2
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let account_rows = sqlx::query(
                r#"
                SELECT id, primary_email, display_name, directory_kind
                FROM accounts
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND gal_visibility = 'tenant'
                ORDER BY lower(display_name) ASC, lower(primary_email) ASC, id ASC
            "#,
            )
            .bind(tenant_id)
            .fetch_all(self.pool())
            .await?;

            let mut entries = account_rows
                .into_iter()
                .map(|row| ExchangeAddressBookEntry {
                    id: row.get("id"),
                    display_name: row.get("display_name"),
                    email: row.get("primary_email"),
                    entry_kind: ExchangeAddressBookEntryKind::Account,
                    directory_kind: directory_kind_from_storage(row.get("directory_kind")),
                    member_emails: Vec::new(),
                    details: ExchangeAddressBookEntryDetails::default(),
                })
                .collect::<Vec<_>>();

            entries.extend(
                self.fetch_accessible_contacts(principal.account_id)
                    .await?
                    .into_iter()
                    .filter(|contact| {
                        !contact.email.trim().is_empty() || !contact.name.trim().is_empty()
                    })
                    .map(|contact| ExchangeAddressBookEntry {
                        id: contact.id,
                        display_name: contact.name.clone(),
                        email: contact.email.clone(),
                        entry_kind: ExchangeAddressBookEntryKind::Contact,
                        directory_kind: ExchangeAddressBookDirectoryKind::Person,
                        member_emails: Vec::new(),
                        details: address_book_details_from_contact(&contact),
                    }),
            );
            let group_rows = sqlx::query(
                r#"
                SELECT id, source, target
                FROM aliases
                WHERE tenant_id = $1
                  AND kind = 'group'
                  AND status = 'active'
                ORDER BY lower(source) ASC, id ASC
                "#,
            )
            .bind(tenant_id)
            .fetch_all(self.pool())
            .await?;
            entries.extend(group_rows.into_iter().map(|row| {
                let source: String = row.get("source");
                let target: String = row.get("target");
                ExchangeAddressBookEntry {
                    id: row.get("id"),
                    display_name: address_book_group_display_name(&source, &target),
                    email: source,
                    entry_kind: ExchangeAddressBookEntryKind::DistributionList,
                    directory_kind: ExchangeAddressBookDirectoryKind::Person,
                    member_emails: vec![target],
                    details: ExchangeAddressBookEntryDetails::default(),
                }
            }));
            entries.sort_by(|left, right| {
                left.display_name
                    .to_ascii_lowercase()
                    .cmp(&right.display_name.to_ascii_lowercase())
                    .then_with(|| {
                        left.email
                            .to_ascii_lowercase()
                            .cmp(&right.email.to_ascii_lowercase())
                    })
                    .then_with(|| (left.entry_kind as u8).cmp(&(right.entry_kind as u8)))
                    .then_with(|| left.id.cmp(&right.id))
            });
            Ok(entries)
        })
    }

    fn fetch_ews_im_list<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsImList> {
        Box::pin(async move {
            let group_rows = sqlx::query(
                r#"
                SELECT id, display_name, modseq
                FROM contact_groups
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND group_kind = 'im_group'
                ORDER BY lower(display_name) ASC, id ASC
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_all(self.pool())
            .await?;
            let groups = group_rows
                .into_iter()
                .map(|row| EwsImGroup {
                    id: row.get("id"),
                    display_name: row.get("display_name"),
                    modseq: row.get("modseq"),
                })
                .collect::<Vec<_>>();
            let group_ids = groups.iter().map(|group| group.id).collect::<Vec<_>>();
            if group_ids.is_empty() {
                return Ok(EwsImList {
                    groups,
                    members: Vec::new(),
                });
            }
            let member_rows = sqlx::query(
                r#"
                SELECT
                    id, contact_group_id, member_kind, contact_id, account_id,
                    external_address, display_name
                FROM contact_group_members
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND contact_group_id = ANY($3)
                ORDER BY contact_group_id ASC, sort_order ASC, lower(display_name) ASC, id ASC
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(&group_ids)
            .fetch_all(self.pool())
            .await?;
            let members = member_rows
                .into_iter()
                .map(|row| EwsImGroupMember {
                    id: row.get("id"),
                    group_id: row.get("contact_group_id"),
                    member_kind: row.get("member_kind"),
                    contact_id: row.get("contact_id"),
                    account_id: row.get("account_id"),
                    external_address: row.get("external_address"),
                    display_name: row.get("display_name"),
                })
                .collect();
            Ok(EwsImList { groups, members })
        })
    }

    fn upsert_ews_im_group<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        display_name: &'a str,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroup> {
        Box::pin(async move {
            let mut tx = self.pool().begin().await?;
            let contact_book_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                INSERT INTO contact_books (id, tenant_id, owner_account_id, display_name, role)
                VALUES ($1, $2, $3, 'IM Contact List', 'im_contact_list')
                ON CONFLICT (tenant_id, owner_account_id, role)
                    WHERE role <> 'custom'
                    DO UPDATE SET display_name = contact_books.display_name
                RETURNING id
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_one(&mut *tx)
            .await?;
            let id = group_id.unwrap_or_else(Uuid::new_v4);
            let row = sqlx::query(
                r#"
                INSERT INTO contact_groups (
                    id, tenant_id, owner_account_id, contact_book_id, display_name,
                    group_kind, source_payload_json
                )
                VALUES ($1, $2, $3, $4, $5, 'im_group', '{"source":"ews"}'::jsonb)
                ON CONFLICT (tenant_id, owner_account_id, id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    modseq = contact_groups.modseq + 1,
                    updated_at = NOW()
                WHERE contact_groups.group_kind = 'im_group'
                RETURNING id, display_name, modseq
                "#,
            )
            .bind(id)
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(contact_book_id)
            .bind(display_name.trim())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("IM group not found"))?;
            tx.commit().await?;
            Ok(EwsImGroup {
                id: row.get("id"),
                display_name: row.get("display_name"),
                modseq: row.get("modseq"),
            })
        })
    }

    fn remove_ews_im_group<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let mut tx = self.pool().begin().await?;
            let deleted = sqlx::query_scalar::<_, Uuid>(
                r#"
                DELETE FROM contact_groups
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND id = $3
                  AND group_kind = 'im_group'
                RETURNING id
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(group_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
            tx.commit().await?;
            Ok(deleted)
        })
    }

    fn add_ews_im_group_member<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Uuid,
        member: EwsImMemberInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroupMember> {
        Box::pin(async move {
            let mut tx = self.pool().begin().await?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM contact_groups
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND id = $3
                  AND group_kind = 'im_group'
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(group_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
            if !exists {
                anyhow::bail!("IM group not found");
            }
            validate_ews_im_member_in_tx(&mut tx, principal, &member).await?;
            let row = insert_ews_im_member_in_tx(&mut tx, principal, group_id, &member).await?;
            tx.commit().await?;
            Ok(EwsImGroupMember {
                id: row.get("id"),
                group_id: row.get("contact_group_id"),
                member_kind: row.get("member_kind"),
                contact_id: row.get("contact_id"),
                account_id: row.get("account_id"),
                external_address: row.get("external_address"),
                display_name: row.get("display_name"),
            })
        })
    }

    fn remove_ews_im_group_member<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        member_kind: &'a str,
        member_value: &'a str,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let mut tx = self.pool().begin().await?;
            let mut query = String::from(
                "DELETE FROM contact_group_members WHERE tenant_id = $1 AND owner_account_id = $2 AND member_kind = $3",
            );
            if group_id.is_some() {
                query.push_str(" AND contact_group_id = $4");
            }
            match member_kind {
                "contact" => query.push_str(if group_id.is_some() {
                    " AND contact_id = $5"
                } else {
                    " AND contact_id = $4"
                }),
                "account" => query.push_str(if group_id.is_some() {
                    " AND account_id = $5"
                } else {
                    " AND account_id = $4"
                }),
                _ => query.push_str(if group_id.is_some() {
                    " AND lower(external_address) = lower($5)"
                } else {
                    " AND lower(external_address) = lower($4)"
                }),
            }
            query.push_str(" RETURNING id");
            let mut q = sqlx::query_scalar::<_, Uuid>(&query)
                .bind(principal.tenant_id)
                .bind(principal.account_id)
                .bind(member_kind);
            if let Some(group_id) = group_id {
                q = q.bind(group_id);
            }
            q = match member_kind {
                "contact" | "account" => q.bind(Uuid::parse_str(member_value)?),
                _ => q.bind(member_value.trim().to_ascii_lowercase()),
            };
            let deleted = q.fetch_all(&mut *tx).await?;
            tx.commit().await?;
            Ok(!deleted.is_empty())
        })
    }

    };
}
