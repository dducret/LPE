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
        "deleted_calendar_event" => MapiIdentityObjectKind::DeletedCalendarEvent,
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
    calendar_folder_ids: &std::collections::HashMap<Uuid, u64>,
    calendar_event_ids: &std::collections::HashMap<Uuid, u64>,
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
            let virtual_metadata =
                crate::mapi_mailstore::virtual_special_folder_metadata(changed_folder_id);
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
                .or_else(|| virtual_metadata.map(|(_, _, _, parent_id, _)| parent_id))
                .or(Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID));
            let display_name = row
                .try_get("object_display_name")
                .ok()
                .or_else(|| virtual_metadata.map(|(_, name, _, _, _)| name.to_string()));
            let parent_display_name = row.try_get("parent_display_name").ok().or_else(|| {
                parent_folder_id
                    .and_then(crate::mapi_mailstore::virtual_special_folder_metadata)
                    .map(|(_, name, _, _, _)| name.to_string())
            });
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
                display_name,
                parent_display_name,
                None,
            ))
            .map(|event| {
                event.with_canonical_ids(
                    row.try_get::<Uuid, _>("object_id").ok(),
                    row.try_get::<Uuid, _>("object_id").ok(),
                )
            })
        }
        "calendar_event" => {
            let event_id = row.try_get::<Uuid, _>("object_id").ok();
            let owner_account_id = row.try_get::<Uuid, _>("owner_account_id").ok();
            let notification_account_id = row.try_get::<Uuid, _>("notification_account_id").ok();
            let calendar_id = row.try_get::<Uuid, _>("calendar_id").ok();
            let calendar_role = row.try_get::<String, _>("calendar_role").ok();
            let event_mapi_object_id = mapi_calendar_event_object_id(
                row.try_get::<Option<i64>, _>("calendar_event_mapi_object_id")
                    .ok()
                    .flatten(),
                event_id,
                calendar_event_ids,
            );
            let (
                Some(event_id),
                Some(owner_account_id),
                Some(notification_account_id),
                Some(calendar_id),
                Some(calendar_role_value),
                Some(event_mapi_object_id),
            ) = (
                event_id,
                owner_account_id,
                notification_account_id,
                calendar_id,
                calendar_role.as_deref(),
                event_mapi_object_id,
            )
            else {
                tracing::warn!(
                    adapter = "mapi",
                    operation = "poll notifications",
                    cursor,
                    canonical_event_id = ?event_id,
                    owner_account_id = ?owner_account_id,
                    notification_account_id = ?notification_account_id,
                    calendar_id = ?calendar_id,
                    calendar_role = ?calendar_role,
                    event_mapi_object_id = ?event_mapi_object_id,
                    "skipping calendar notification with incomplete durable identity metadata"
                );
                return None;
            };
            let old_calendar_role = row
                .try_get::<Option<String>, _>("old_calendar_role")
                .ok()
                .flatten();
            let old_calendar_id = row
                .try_get::<Option<Uuid>, _>("old_calendar_id")
                .ok()
                .flatten();
            let notification = mapi_calendar_notification_event(
                MapiCalendarNotificationData {
                    cursor,
                    modseq,
                    change_kind: &change_kind,
                    notification_account_id,
                    owner_account_id,
                    calendar_id,
                    calendar_role: calendar_role_value,
                    old_calendar_id,
                    old_calendar_role: old_calendar_role.as_deref(),
                    event_id,
                    event_mapi_object_id,
                    subject: row
                        .try_get::<Option<String>, _>("calendar_event_subject")
                        .ok()
                        .flatten(),
                },
                calendar_folder_ids,
            );
            if notification.is_none() {
                tracing::warn!(
                    adapter = "mapi",
                    operation = "poll notifications",
                    cursor,
                    canonical_event_id = %event_id,
                    change_kind,
                    calendar_id = %calendar_id,
                    calendar_role = calendar_role_value,
                    old_calendar_id = ?old_calendar_id,
                    old_calendar_role = ?old_calendar_role,
                    "skipping calendar notification whose folder semantics are incomplete"
                );
            }
            notification
        }
        "deleted_calendar_event" => {
            let event_id = row.try_get::<Uuid, _>("object_id").ok()?;
            let owner_account_id = row.try_get::<Uuid, _>("owner_account_id").ok()?;
            let notification_account_id =
                row.try_get::<Uuid, _>("notification_account_id").ok()?;
            let old_calendar_id = row
                .try_get::<Option<Uuid>, _>("old_calendar_id")
                .ok()
                .flatten()?;
            let old_calendar_role = row
                .try_get::<Option<String>, _>("old_calendar_role")
                .ok()
                .flatten()?;
            let new_message_id = row
                .try_get::<Option<i64>, _>("calendar_event_mapi_object_id")
                .ok()
                .flatten()? as u64;
            let old_message_id = row
                .try_get::<Option<i64>, _>("old_calendar_event_mapi_object_id")
                .ok()
                .flatten()? as u64;
            let old_collection_id = mapi_calendar_collection_id(
                notification_account_id,
                owner_account_id,
                old_calendar_id,
                &old_calendar_role,
            );
            let old_folder_id = mapi_calendar_notification_folder_id(
                &old_collection_id,
                calendar_folder_ids,
            )?;
            Some(
                MapiNotificationEvent::canonical(
                    MapiNotificationKind::Content,
                    mapi_notification_event_mask_for_change("moved", false),
                    crate::mapi::identity::TRASH_FOLDER_ID,
                    Some(new_message_id),
                    Some(old_folder_id),
                    cursor,
                    modseq,
                    None,
                    None,
                    "moved".to_string(),
                    None,
                    None,
                    row.try_get::<Option<String>, _>("calendar_event_subject")
                        .ok()
                        .flatten(),
                )
                .with_old_message_id(Some(old_message_id))
                .with_canonical_ids(None, Some(event_id))
                .with_object_kind("deleted_calendar_event"),
            )
        }
        "navigation_shortcut" => {
            let shortcut_id = row.try_get::<Uuid, _>("object_id").ok()?;
            let message_id = row
                .try_get::<Option<i64>, _>("navigation_shortcut_mapi_object_id")
                .ok()
                .flatten()? as u64;
            // [MS-OXOCFG] sections 2.2.9 and 3.1.4.9 store WLinks as FAI
            // messages in Common Views. [MS-OXCNOTIF] sections 2.2.1.1,
            // 2.2.1.1.1, and 3.1.4.3 make their durable object changes drive
            // the subscribed associated contents table notification.
            Some(
                MapiNotificationEvent::canonical(
                    MapiNotificationKind::Content,
                    mapi_notification_event_mask_for_change(&change_kind, false),
                    crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                    Some(message_id),
                    None,
                    cursor,
                    modseq,
                    None,
                    None,
                    change_kind,
                    None,
                    None,
                    None,
                )
                .with_canonical_ids(None, Some(shortcut_id))
                .with_object_kind("navigation_shortcut"),
            )
        }
        "associated_config" => {
            let config_id = row.try_get::<Uuid, _>("object_id").ok()?;
            let message_id = row
                .try_get::<Option<i64>, _>("associated_config_mapi_object_id")
                .ok()
                .flatten()? as u64;
            let folder_id = row
                .try_get::<serde_json::Value, _>("summary_json")
                .ok()?
                .get("folderId")?
                .as_str()?
                .parse::<u64>()
                .ok()?;
            // [MS-OXCFOLD] section 2.2.1.14 stores configuration messages as
            // FAI rows. [MS-OXCNOTIF] sections 2.2.1.1 and 3.1.4.3 require
            // their durable changes to drive the subscribed associated table.
            Some(
                MapiNotificationEvent::canonical(
                    MapiNotificationKind::Content,
                    mapi_notification_event_mask_for_change(&change_kind, false),
                    folder_id,
                    Some(message_id),
                    None,
                    cursor,
                    modseq,
                    None,
                    None,
                    change_kind,
                    None,
                    None,
                    None,
                )
                .with_canonical_ids(None, Some(config_id))
                .with_object_kind("associated_config"),
            )
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
                row.try_get::<i64, _>("scope_parent_mapi_object_id").ok(),
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

fn mapi_calendar_event_object_id(
    durable_object_id: Option<i64>,
    event_id: Option<Uuid>,
    scoped_event_ids: &std::collections::HashMap<Uuid, u64>,
) -> Option<u64> {
    durable_object_id
        .map(|value| value as u64)
        .or_else(|| event_id.and_then(|event_id| scoped_event_ids.get(&event_id).copied()))
}

struct MapiCalendarNotificationData<'a> {
    cursor: i64,
    modseq: u64,
    change_kind: &'a str,
    notification_account_id: Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    calendar_role: &'a str,
    old_calendar_id: Option<Uuid>,
    old_calendar_role: Option<&'a str>,
    event_id: Uuid,
    event_mapi_object_id: u64,
    subject: Option<String>,
}

/// [MS-OXCNOTIF] sections 2.2.1.1 and 2.2.1.4.1.2 model Calendar items as
/// message objects: ObjectCreated/ObjectDeleted/ObjectModified carry FolderId
/// followed by MessageId when the message-event flag is set.
fn mapi_calendar_notification_event(
    data: MapiCalendarNotificationData<'_>,
    calendar_folder_ids: &std::collections::HashMap<Uuid, u64>,
) -> Option<MapiNotificationEvent> {
    // [MS-OXCNOTIF] section 2.2.1.4.1.2 defines the destination MessageId and
    // source OldMessageId fields. [MS-OXCFXICS] section 3.1.5.3 requires a new
    // internal ID for an inter-folder move, so those two values are distinct.
    // The durable Calendar change row currently exposes only the destination
    // identity; fail closed instead of serializing that MID as both values.
    if data.change_kind == "moved" {
        return None;
    }
    let collection_id = mapi_calendar_collection_id(
        data.notification_account_id,
        data.owner_account_id,
        data.calendar_id,
        data.calendar_role,
    );
    let folder_id = mapi_calendar_notification_folder_id(&collection_id, calendar_folder_ids)?;
    let old_folder_id = match data.change_kind {
        "moved" => {
            let old_collection_id = mapi_calendar_collection_id(
                data.notification_account_id,
                data.owner_account_id,
                data.old_calendar_id?,
                data.old_calendar_role?,
            );
            mapi_calendar_notification_folder_id(&old_collection_id, calendar_folder_ids)
        }
        _ => None,
    };
    Some(
        MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            mapi_notification_event_mask_for_change(data.change_kind, false),
            folder_id,
            Some(data.event_mapi_object_id),
            old_folder_id,
            data.cursor,
            data.modseq,
            None,
            None,
            data.change_kind.to_string(),
            None,
            None,
            data.subject,
        )
        .with_canonical_ids(Some(data.calendar_id), Some(data.event_id))
        .with_object_kind("calendar_event"),
    )
}

fn mapi_calendar_notification_folder_id(
    collection_id: &str,
    calendar_folder_ids: &std::collections::HashMap<Uuid, u64>,
) -> Option<u64> {
    let kind = crate::mapi_store::MapiCollaborationFolderKind::Calendar;
    match crate::mapi_store::collaboration_folder_identity_canonical_id_for_collection(
        kind,
        collection_id,
    ) {
        Some(canonical_id) => calendar_folder_ids.get(&canonical_id).copied(),
        None => crate::mapi_store::mapi_collaboration_folder_id_for_collection(kind, collection_id),
    }
}

fn mapi_calendar_notification_folder_identity_ids_from_row(
    row: &sqlx::postgres::PgRow,
) -> Vec<Uuid> {
    let Some(notification_account_id) = row
        .try_get::<Option<Uuid>, _>("notification_account_id")
        .ok()
        .flatten()
    else {
        return Vec::new();
    };
    let Some(owner_account_id) = row
        .try_get::<Option<Uuid>, _>("owner_account_id")
        .ok()
        .flatten()
    else {
        return Vec::new();
    };
    let mut identity_ids = Vec::new();
    let mut append_identity = |calendar_id: Uuid, calendar_role: &str| {
        let collection_id = mapi_calendar_collection_id(
            notification_account_id,
            owner_account_id,
            calendar_id,
            calendar_role,
        );
        if let Some(canonical_id) =
            crate::mapi_store::collaboration_folder_identity_canonical_id_for_collection(
                crate::mapi_store::MapiCollaborationFolderKind::Calendar,
                &collection_id,
            )
        {
            push_unique_uuid(&mut identity_ids, canonical_id);
        }
    };
    if let (Some(calendar_id), Some(calendar_role)) = (
        row.try_get::<Option<Uuid>, _>("calendar_id").ok().flatten(),
        row.try_get::<Option<String>, _>("calendar_role")
            .ok()
            .flatten(),
    ) {
        append_identity(calendar_id, &calendar_role);
    }
    if row.get::<String, _>("change_kind") == "moved"
        || row.get::<String, _>("object_kind") == "deleted_calendar_event"
    {
        if let (Some(calendar_id), Some(calendar_role)) = (
            row.try_get::<Option<Uuid>, _>("old_calendar_id")
                .ok()
                .flatten(),
            row.try_get::<Option<String>, _>("old_calendar_role")
                .ok()
                .flatten(),
        ) {
            append_identity(calendar_id, &calendar_role);
        }
    }
    identity_ids
}

fn mapi_calendar_collection_id(
    notification_account_id: Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    calendar_role: &str,
) -> String {
    if calendar_role == "custom" {
        calendar_id.to_string()
    } else if notification_account_id == owner_account_id {
        "default".to_string()
    } else {
        format!("shared-calendar-{owner_account_id}")
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
    use super::{
        mapi_calendar_event_object_id, mapi_calendar_notification_event,
        mapi_notification_event_mask_for_change, MapiCalendarNotificationData,
    };
    use crate::mapi::notifications::MapiNotificationKind;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn inbox_delivery_uses_new_mail_notification_mask() {
        assert_eq!(
            mapi_notification_event_mask_for_change("created", true),
            0x0002
        );
        assert_eq!(
            mapi_notification_event_mask_for_change("created", false),
            0x0004
        );
    }

    #[test]
    fn calendar_create_update_delete_notifications_keep_stable_fid_mid() {
        let account_id = Uuid::from_u128(0x1020);
        let calendar_id = Uuid::from_u128(0x3040);
        let event_id = Uuid::from_u128(0x5060);
        let event_mapi_object_id = 0x0000_0000_1234_0001;
        let calendar_folder_ids = HashMap::new();
        let event = |change_kind| {
            mapi_calendar_notification_event(
                MapiCalendarNotificationData {
                    cursor: 42,
                    modseq: 7,
                    change_kind,
                    notification_account_id: account_id,
                    owner_account_id: account_id,
                    calendar_id,
                    calendar_role: "calendar",
                    old_calendar_id: None,
                    old_calendar_role: None,
                    event_id,
                    event_mapi_object_id,
                    subject: Some("Outlook Calendar regression".to_string()),
                },
                &calendar_folder_ids,
            )
            .expect("default Calendar notification")
        };

        // [MS-OXCNOTIF] sections 2.2.1.1 and 2.2.1.4.1.2 define the bounded
        // FolderId/MessageId fields for these non-move object notifications.
        // The durable principal-scoped Event identity supplies those values.
        for (change_kind, expected_mask) in [
            ("created", 0x0004),
            ("updated", 0x0010),
            ("destroyed", 0x0008),
        ] {
            let notification = event(change_kind);
            assert_eq!(
                notification.notification_test_shape(),
                (
                    MapiNotificationKind::Content,
                    expected_mask,
                    crate::mapi::identity::CALENDAR_FOLDER_ID,
                    Some(event_mapi_object_id),
                    None,
                    None,
                    Some("calendar_event"),
                )
            );
            assert_eq!(notification.canonical_folder_id(), Some(calendar_id));
            assert_eq!(notification.canonical_message_id(), Some(event_id));
        }
    }

    #[test]
    fn calendar_move_is_suppressed_without_a_distinct_old_message_id() {
        let account_id = Uuid::from_u128(0x7080);
        let default_calendar_id = Uuid::from_u128(0x90a0);
        let custom_calendar_id = Uuid::from_u128(0xb0c0);
        let event_mapi_object_id = 0x0000_0000_5678_0001;
        let expected_custom_folder_id = 0x0000_0000_9abc_0001;
        let custom_folder_canonical_id =
            crate::mapi_store::collaboration_folder_identity_canonical_id_for_collection(
                crate::mapi_store::MapiCollaborationFolderKind::Calendar,
                &custom_calendar_id.to_string(),
            )
            .expect("custom Calendar folder canonical identity");
        let calendar_folder_ids =
            HashMap::from([(custom_folder_canonical_id, expected_custom_folder_id)]);

        let notification = mapi_calendar_notification_event(
            MapiCalendarNotificationData {
                cursor: 43,
                modseq: 8,
                change_kind: "moved",
                notification_account_id: account_id,
                owner_account_id: account_id,
                calendar_id: custom_calendar_id,
                calendar_role: "custom",
                old_calendar_id: Some(default_calendar_id),
                old_calendar_role: Some("calendar"),
                event_id: Uuid::from_u128(0xd0e0),
                event_mapi_object_id,
                subject: Some("Calendar move regression".to_string()),
            },
            &calendar_folder_ids,
        );

        assert!(notification.is_none());
    }

    #[test]
    fn calendar_notification_identity_never_falls_back_to_another_principal_cache_entry() {
        let event_id = Uuid::from_u128(0xe0f0);
        let foreign_object_id = 0x0000_0000_1111_0001;
        let scoped_object_id = 0x0000_0000_2222_0001;
        crate::mapi::identity::remember_mapi_identity(event_id, foreign_object_id);

        let mut scoped_event_ids = HashMap::new();
        assert_eq!(
            mapi_calendar_event_object_id(None, Some(event_id), &scoped_event_ids),
            None
        );

        scoped_event_ids.insert(event_id, scoped_object_id);
        assert_eq!(
            mapi_calendar_event_object_id(None, Some(event_id), &scoped_event_ids),
            Some(scoped_object_id)
        );
        crate::mapi::identity::forget_mapi_identity(&event_id);
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
