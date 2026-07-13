fn task_matches_collection(task: &ClientTask, collection_id: &str) -> bool {
    matches!(collection_id, "tasks" | "default") || task.task_list_id.to_string() == collection_id
}

fn directory_kind_from_storage(value: String) -> ExchangeAddressBookDirectoryKind {
    match value.as_str() {
        "room" => ExchangeAddressBookDirectoryKind::Room,
        "equipment" => ExchangeAddressBookDirectoryKind::Equipment,
        _ => ExchangeAddressBookDirectoryKind::Person,
    }
}

fn address_book_details_from_contact(
    contact: &AccessibleContact,
) -> ExchangeAddressBookEntryDetails {
    ExchangeAddressBookEntryDetails {
        given_name: contact.structured_name.given.clone(),
        surname: contact.structured_name.family.clone(),
        nickname: contact.structured_name.nickname.clone(),
        primary_phone: contact.phone.clone(),
        mobile_phone: contact_phone_by_label(contact, &["mobile", "cell"]),
        home_phone: contact_phone_by_label(contact, &["home"]),
        business2_phones: contact_phone_values_by_label(contact, &["work2", "business2"]),
        company_name: contact.organization_name.clone(),
        title: contact.job_title.clone(),
        department_name: contact.team.clone(),
        postal_address: contact_address_value(contact, &["full", "address"]),
        street_address: contact_address_value(contact, &["street", "streetAddress", "address"]),
        locality: contact_address_value(contact, &["city", "locality"]),
        state_or_province: contact_address_value(contact, &["state", "region", "stateOrProvince"]),
        country: contact_address_value(contact, &["country"]),
        postal_code: contact_address_value(contact, &["postcode", "postalCode", "zip"]),
        phonetic_given_name: contact.structured_name.phonetic_given.clone(),
        phonetic_surname: contact.structured_name.phonetic_family.clone(),
    }
}

fn contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    contact_phone_values_by_label(contact, labels)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn contact_phone_values_by_label(contact: &AccessibleContact, labels: &[&str]) -> Vec<String> {
    contact_labeled_json_values(&contact.phones_json, "phone", labels)
}

fn contact_labeled_json_values(
    value: &serde_json::Value,
    key: &str,
    labels: &[&str],
) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            let label = item
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            labels
                .iter()
                .any(|expected| label.eq_ignore_ascii_case(expected))
        })
        .filter_map(|item| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn contact_address_value(contact: &AccessibleContact, keys: &[&str]) -> String {
    contact
        .addresses_json
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|item| {
            keys.iter()
                .filter_map(|key| item.get(*key).and_then(serde_json::Value::as_str))
                .map(str::trim)
                .find(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .unwrap_or_default()
}

fn address_book_group_display_name(source: &str, target: &str) -> String {
    let target = target.trim();
    if !target.is_empty() && !target.eq_ignore_ascii_case(source.trim()) {
        return target.to_string();
    }
    source
        .split_once('@')
        .map(|(local_part, _)| local_part)
        .filter(|local_part| !local_part.trim().is_empty())
        .unwrap_or(source)
        .to_string()
}

async fn mapi_tenant_id_for_account(storage: &Storage, account_id: Uuid) -> Result<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .fetch_optional(storage.pool())
    .await?
    .ok_or_else(|| anyhow::anyhow!("account not found"))
}

fn mapi_identity_lookup_from_row(row: sqlx::postgres::PgRow) -> Result<MapiIdentityLookupRecord> {
    let object_kind = match row.get::<String, _>("object_kind").as_str() {
        "account" => MapiIdentityObjectKind::Account,
        "mailbox" => MapiIdentityObjectKind::Mailbox,
        "message" => MapiIdentityObjectKind::Message,
        "contact" => MapiIdentityObjectKind::Contact,
        "calendar_event" => MapiIdentityObjectKind::CalendarEvent,
        "task" => MapiIdentityObjectKind::Task,
        "note" => MapiIdentityObjectKind::Note,
        "journal_entry" => MapiIdentityObjectKind::JournalEntry,
        "search_folder_definition" => MapiIdentityObjectKind::SearchFolderDefinition,
        "conversation_action" => MapiIdentityObjectKind::ConversationAction,
        "navigation_shortcut" => MapiIdentityObjectKind::NavigationShortcut,
        "associated_config" => MapiIdentityObjectKind::AssociatedConfig,
        "delegate_freebusy_message" => MapiIdentityObjectKind::DelegateFreeBusyMessage,
        value => anyhow::bail!("unsupported MAPI object kind: {value}"),
    };
    Ok(MapiIdentityLookupRecord {
        object_kind,
        canonical_id: row.get("canonical_id"),
        object_id: row.get::<i64, _>("mapi_object_id") as u64,
        source_key: row.get("source_key"),
    })
}

fn mapi_notification_event_from_change_row(
    row: sqlx::postgres::PgRow,
) -> Option<MapiNotificationEvent> {
    let object_kind = row.get::<String, _>("object_kind");
    let change_kind = row.get::<String, _>("change_kind");
    let cursor = row.get::<i64, _>("cursor");
    let modseq = row.get::<i64, _>("modseq").max(0) as u64;
    match object_kind.as_str() {
        "mailbox" => {
            let event_mask = mapi_notification_event_mask_for_change(&change_kind, false);
            let changed_folder_id = mapi_folder_id_from_role_or_identity(
                row.try_get::<String, _>("object_role").ok().as_deref(),
                row.try_get::<i64, _>("object_mapi_object_id").ok(),
            )?;
            let parent_folder_id = row
                .try_get::<String, _>("parent_role")
                .ok()
                .as_deref()
                .and_then(crate::mapi_store::reserved_folder_counter_for_role)
                .map(crate::mapi::identity::mapi_store_id)
                .or_else(|| {
                    row.try_get::<i64, _>("parent_mapi_object_id")
                        .ok()
                        .map(|value| value as u64)
                })
                .or(Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID));
            Some(MapiNotificationEvent::canonical(
                MapiNotificationKind::Hierarchy,
                event_mask,
                parent_folder_id?,
                Some(changed_folder_id),
                None,
                cursor,
                modseq,
                row.try_get("object_total_messages").ok(),
                row.try_get("object_unread_messages").ok(),
                change_kind,
                row.try_get("object_display_name").ok(),
                row.try_get("parent_display_name").ok(),
                None,
            ))
            .map(|event| {
                event.with_canonical_ids(
                    row.try_get::<Uuid, _>("object_id").ok(),
                    row.try_get::<Uuid, _>("object_id").ok(),
                )
            })
        }
        "mailbox_message" | "attachment" => {
            let scope_role = row.try_get::<String, _>("scope_role").ok();
            // [MS-OXCNOTIF] 2.2.1.1 and section 4 distinguish a delivered
            // new message (0x0002) from an object created by a client (0x0004).
            let is_new_mail = object_kind == "mailbox_message"
                && change_kind == "created"
                && scope_role.as_deref() == Some("inbox");
            let event_mask = mapi_notification_event_mask_for_change(&change_kind, is_new_mail);
            let folder_id = mapi_folder_id_from_role_or_identity(
                scope_role.as_deref(),
                row.try_get::<i64, _>("scope_mapi_object_id").ok(),
            )?;
            let parent_folder_id = mapi_folder_id_from_role_or_identity(
                row.try_get::<String, _>("scope_parent_role")
                    .ok()
                    .as_deref(),
                row.try_get::<i64, _>("scope_parent_mapi_object_id")
                    .ok(),
            )
            .or(Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID));
            Some(MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                event_mask,
                folder_id,
                row.try_get::<i64, _>("message_mapi_object_id")
                    .ok()
                    .map(|value| value as u64),
                row.try_get::<i64, _>("source_mapi_object_id")
                    .ok()
                    .map(|value| value as u64),
                cursor,
                modseq,
                row.try_get("scope_total_messages").ok(),
                row.try_get("scope_unread_messages").ok(),
                change_kind,
                row.try_get("scope_display_name").ok(),
                row.try_get("source_display_name").ok(),
                row.try_get("message_subject").ok(),
            ))
            .map(|event| {
                event.with_canonical_ids(
                    row.try_get::<Uuid, _>("mailbox_id").ok(),
                    row.try_get::<Uuid, _>("message_id").ok(),
                )
            })
            .map(|event| event.with_parent_folder_id(parent_folder_id))
        }
        _ => None,
    }
}

fn mapi_folder_id_from_role_or_identity(role: Option<&str>, identity: Option<i64>) -> Option<u64> {
    role.and_then(crate::mapi_store::reserved_folder_counter_for_role)
        .map(crate::mapi::identity::mapi_store_id)
        .or_else(|| identity.map(|value| value as u64))
}

fn mapi_notification_event_mask_for_change(change_kind: &str, is_new_mail: bool) -> u16 {
    match change_kind {
        "created" if is_new_mail => 0x0002,
        "created" => 0x0004,
        "destroyed" | "deleted" | "expunged" => 0x0008,
        "moved" => 0x0020,
        _ => 0x0010,
    }
}

#[cfg(test)]
mod notification_tests {
    use super::mapi_notification_event_mask_for_change;

    #[test]
    fn inbox_delivery_uses_new_mail_notification_mask() {
        assert_eq!(mapi_notification_event_mask_for_change("created", true), 0x0002);
        assert_eq!(mapi_notification_event_mask_for_change("created", false), 0x0004);
    }
}

#[allow(dead_code)]
fn mapi_sync_checkpoint_from_row(row: sqlx::postgres::PgRow) -> Result<MapiSyncCheckpoint> {
    let checkpoint_kind = match row.get::<String, _>("checkpoint_kind").as_str() {
        "hierarchy" => MapiCheckpointKind::Hierarchy,
        "content" => MapiCheckpointKind::Content,
        "read_state" => MapiCheckpointKind::ReadState,
        value => anyhow::bail!("unsupported MAPI checkpoint kind: {value}"),
    };
    Ok(MapiSyncCheckpoint {
        mailbox_id: row.get("mailbox_id"),
        checkpoint_kind,
        last_change_sequence: row.get::<i64, _>("last_change_sequence") as u64,
        last_modseq: row.get::<i64, _>("last_modseq") as u64,
        cursor_json: row.get("cursor_json"),
    })
}

fn push_unique_uuid(values: &mut Vec<Uuid>, value: Uuid) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn push_unique_associated_config_change(
    values: &mut Vec<MapiAssociatedConfigChange>,
    folder_id: u64,
    config_id: Uuid,
) {
    if !values
        .iter()
        .any(|value| value.folder_id == folder_id && value.config_id == config_id)
    {
        values.push(MapiAssociatedConfigChange {
            folder_id,
            config_id,
        });
    }
}

async fn ews_mail_app_catalog_id(
    storage: &Storage,
    principal: &AccountPrincipal,
    app_id: &str,
) -> Result<Uuid> {
    let app_id = app_id.trim();
    if app_id.is_empty() {
        anyhow::bail!("mail app id is required");
    }
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mail_app_catalog
        WHERE tenant_id = $1
          AND app_id = $2
          AND lifecycle_state = 'active'
        LIMIT 1
        "#,
    )
    .bind(principal.tenant_id)
    .bind(app_id)
    .fetch_optional(storage.pool())
    .await?
    .ok_or_else(|| anyhow::anyhow!("mail app not found"))
}

async fn ews_update_mail_app_install_status(
    storage: &Storage,
    principal: &AccountPrincipal,
    app_id: &str,
    status: &str,
    audit: AuditEntryInput,
) -> Result<EwsMailAppInstall> {
    let catalog_id = ews_mail_app_catalog_id(storage, principal, app_id).await?;
    let row = sqlx::query(
        r#"
        UPDATE mail_app_installations
        SET status = $4,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND app_catalog_id = $3
          AND install_scope = 'account'
          AND status <> 'uninstalled'
        RETURNING app_catalog_id, status
        "#,
    )
    .bind(principal.tenant_id)
    .bind(principal.account_id)
    .bind(catalog_id)
    .bind(status)
    .fetch_optional(storage.pool())
    .await?
    .ok_or_else(|| anyhow::anyhow!("mail app installation not found"))?;
    storage
        .append_audit_event(principal.tenant_id, audit)
        .await?;
    Ok(EwsMailAppInstall {
        catalog_id: row.try_get("app_catalog_id")?,
        app_id: app_id.trim().to_string(),
        status: row.try_get("status")?,
    })
}

async fn validate_ews_im_member_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal: &AccountPrincipal,
    member: &EwsImMemberInput,
) -> Result<()> {
    match member.member_kind.as_str() {
        "contact" => {
            let contact_id = member
                .contact_id
                .ok_or_else(|| anyhow::anyhow!("contact member id is required"))?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT c.id
                FROM contacts c
                JOIN contact_books b
                  ON b.tenant_id = c.tenant_id
                 AND b.owner_account_id = c.owner_account_id
                 AND b.id = c.contact_book_id
                LEFT JOIN contact_book_grants g
                  ON g.tenant_id = b.tenant_id
                 AND g.contact_book_id = b.id
                 AND g.grantee_account_id = $2
                WHERE c.tenant_id = $1
                  AND c.id = $3
                  AND (c.owner_account_id = $2 OR g.may_read = TRUE)
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(contact_id)
            .fetch_optional(&mut **tx)
            .await?
            .is_some();
            if !exists {
                anyhow::bail!("contact member not found");
            }
        }
        "account" => {
            let account_id = member
                .account_id
                .ok_or_else(|| anyhow::anyhow!("account member id is required"))?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM accounts
                WHERE tenant_id = $1
                  AND id = $2
                  AND status = 'active'
                  AND gal_visibility = 'tenant'
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(account_id)
            .fetch_optional(&mut **tx)
            .await?
            .is_some();
            if !exists {
                anyhow::bail!("account member not found");
            }
        }
        "distribution_group" | "tel_uri" => {
            if member
                .external_address
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                anyhow::bail!("external member address is required");
            }
        }
        _ => anyhow::bail!("unsupported IM member kind"),
    }
    Ok(())
}

async fn insert_ews_im_member_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal: &AccountPrincipal,
    group_id: Uuid,
    member: &EwsImMemberInput,
) -> Result<sqlx::postgres::PgRow> {
    let display_name = member.display_name.trim();
    match member.member_kind.as_str() {
        "contact" => sqlx::query(
            r#"
            INSERT INTO contact_group_members (
                id, tenant_id, owner_account_id, contact_group_id, member_kind,
                contact_id, display_name
            )
            VALUES ($1, $2, $3, $4, 'contact', $5, $6)
            ON CONFLICT (tenant_id, owner_account_id, contact_group_id, contact_id)
                WHERE member_kind = 'contact'
                DO UPDATE SET display_name = EXCLUDED.display_name
            RETURNING
                id, contact_group_id, member_kind, contact_id, account_id,
                external_address, display_name
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(principal.tenant_id)
        .bind(principal.account_id)
        .bind(group_id)
        .bind(member.contact_id)
        .bind(display_name)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into),
        "account" => sqlx::query(
            r#"
            INSERT INTO contact_group_members (
                id, tenant_id, owner_account_id, contact_group_id, member_kind,
                account_id, display_name
            )
            VALUES ($1, $2, $3, $4, 'account', $5, $6)
            ON CONFLICT (tenant_id, owner_account_id, contact_group_id, account_id)
                WHERE member_kind = 'account'
                DO UPDATE SET display_name = EXCLUDED.display_name
            RETURNING
                id, contact_group_id, member_kind, contact_id, account_id,
                external_address, display_name
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(principal.tenant_id)
        .bind(principal.account_id)
        .bind(group_id)
        .bind(member.account_id)
        .bind(display_name)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into),
        "distribution_group" | "tel_uri" => {
            let external_address = member
                .external_address
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if let Some(existing_id) = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM contact_group_members
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND contact_group_id = $3
                  AND member_kind = $4
                  AND lower(external_address) = $5
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(group_id)
            .bind(&member.member_kind)
            .bind(&external_address)
            .fetch_optional(&mut **tx)
            .await?
            {
                return sqlx::query(
                    r#"
                    UPDATE contact_group_members
                    SET display_name = $1
                    WHERE id = $2
                      AND tenant_id = $3
                      AND owner_account_id = $4
                    RETURNING
                        id, contact_group_id, member_kind, contact_id, account_id,
                        external_address, display_name
                    "#,
                )
                .bind(display_name)
                .bind(existing_id)
                .bind(principal.tenant_id)
                .bind(principal.account_id)
                .fetch_one(&mut **tx)
                .await
                .map_err(Into::into);
            }

            sqlx::query(
                r#"
                INSERT INTO contact_group_members (
                    id, tenant_id, owner_account_id, contact_group_id, member_kind,
                    external_address, display_name
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING
                    id, contact_group_id, member_kind, contact_id, account_id,
                    external_address, display_name
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(group_id)
            .bind(&member.member_kind)
            .bind(&external_address)
            .bind(display_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(Into::into)
        }
        _ => anyhow::bail!("unsupported IM member kind"),
    }
}

fn ews_unified_messaging_call_select_sql() -> &'static str {
    r#"
    SELECT id, call_id, call_kind, status, phone_number, message_id,
           to_char(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
           to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
    FROM unified_messaging_calls
    WHERE tenant_id = $1
      AND account_id = $2
      AND call_id = $3
    LIMIT 1
    "#
}

fn ews_unified_messaging_call_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<EwsUnifiedMessagingCall> {
    Ok(EwsUnifiedMessagingCall {
        id: row.try_get("id")?,
        call_id: row.try_get("call_id")?,
        call_kind: row.try_get("call_kind")?,
        status: row.try_get("status")?,
        phone_number: row.try_get("phone_number")?,
        message_id: row.try_get("message_id")?,
        requested_at: row.try_get("requested_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
