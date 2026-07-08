use super::*;

pub(super) fn split_custom_property_values(
    values: Vec<(u32, MapiValue)>,
) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>) {
    values
        .into_iter()
        .partition(|(tag, _)| !is_custom_property_tag(*tag))
}

pub(super) fn split_object_property_values(
    object: &MapiObject,
    values: Vec<(u32, MapiValue)>,
) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>) {
    if !matches!(object, MapiObject::AssociatedConfig { .. }) {
        return split_custom_property_values(values);
    }
    (values, Vec::new())
}

pub(super) fn apply_mapi_property_values_to_map(
    properties: &mut HashMap<u32, MapiValue>,
    values: Vec<(u32, MapiValue)>,
) {
    properties.extend(
        values
            .into_iter()
            .map(|(tag, value)| (canonical_property_storage_tag(tag), value)),
    );
}

pub(super) async fn upsert_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiCustomPropertyObjectKind,
    canonical_id: Uuid,
    values: Vec<(u32, MapiValue)>,
) -> Result<()>
where
    S: ExchangeStore,
{
    if values.is_empty() {
        return Ok(());
    }
    let values = values
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            MapiCustomPropertyValue {
                property_tag,
                property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    store
        .upsert_mapi_custom_property_values(
            principal.account_id,
            object_kind,
            canonical_id,
            &values,
        )
        .await
}

pub(super) async fn upsert_custom_property_values_from_map<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiCustomPropertyObjectKind,
    canonical_id: Uuid,
    properties: &HashMap<u32, MapiValue>,
) -> Result<()>
where
    S: ExchangeStore,
{
    let values = properties
        .iter()
        .filter(|(tag, _value)| is_custom_property_tag(**tag))
        .map(|(tag, value)| (*tag, value.clone()))
        .collect::<Vec<_>>();
    upsert_custom_property_values(store, principal, object_kind, canonical_id, values).await
}

pub(super) async fn fetch_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<HashMap<u32, Vec<u8>>>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(HashMap::new());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(HashMap::new());
    };
    Ok(store
        .fetch_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value.property_value))
        .collect())
}

pub(super) async fn copy_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Option<Vec<(usize, u32, u32)>>>
where
    S: ExchangeStore,
{
    if property_tags.is_empty() || !property_tags.iter().copied().all(is_custom_property_tag) {
        return Ok(None);
    }
    let Some((source_kind, source_id)) =
        custom_property_object_identity(source, mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination, mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    let source_values = store
        .fetch_mapi_custom_property_values(
            principal.account_id,
            source_kind,
            source_id,
            property_tags,
        )
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value))
        .collect::<HashMap<_, _>>();
    let mut source_values = source_values;
    for value in staged_custom_property_values(source, Some(property_tags)) {
        source_values.insert(value.property_tag, value);
    }
    let mut copied_values = Vec::new();
    let mut problems = Vec::new();
    for (index, property_tag) in property_tags.iter().copied().enumerate() {
        if let Some(value) = source_values.get(&property_tag) {
            copied_values.push(MapiCustomPropertyValue {
                property_tag,
                property_type: value.property_type,
                property_value: value.property_value.clone(),
            });
        } else {
            problems.push((index, property_tag, 0x8004_010F));
        }
    }
    if !copied_values.is_empty() {
        store
            .upsert_mapi_custom_property_values(
                principal.account_id,
                destination_kind,
                destination_id,
                &copied_values,
            )
            .await?;
    }
    Ok(Some(problems))
}

pub(super) async fn copy_all_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    excluded_property_tags: &[u32],
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some((source_kind, source_id)) =
        custom_property_object_identity(source, mailboxes, emails, snapshot)
    else {
        return Ok(false);
    };
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination, mailboxes, emails, snapshot)
    else {
        return Ok(false);
    };
    let excluded = excluded_property_tags
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut values = store
        .fetch_all_mapi_custom_property_values(principal.account_id, source_kind, source_id)
        .await?
        .into_iter()
        .chain(staged_custom_property_values(source, None).into_iter())
        .filter(|value| !excluded.contains(&value.property_tag))
        .map(|value| (value.property_tag, value))
        .collect::<HashMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.property_tag);
    if values.is_empty() {
        return Ok(false);
    }
    store
        .upsert_mapi_custom_property_values(
            principal.account_id,
            destination_kind,
            destination_id,
            &values,
        )
        .await?;
    Ok(true)
}

pub(super) async fn delete_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<()>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(());
    };
    store
        .delete_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await
}

pub(super) fn custom_property_object_identity(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(MapiCustomPropertyObjectKind, Uuid)> {
    match object? {
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        } => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or(saved_email.as_ref().map(|saved| &saved.email))
            .map(|email| (MapiCustomPropertyObjectKind::Message, email.id)),
        MapiObject::Contact {
            folder_id,
            contact_id,
        } => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| (MapiCustomPropertyObjectKind::Contact, contact.canonical_id)),
        MapiObject::Event {
            folder_id,
            event_id,
        } if !mapi_calendar_content_items_suppressed(*folder_id, snapshot) => {
            snapshot.event_for_id(*folder_id, *event_id).map(|event| {
                (
                    MapiCustomPropertyObjectKind::CalendarEvent,
                    event.canonical_id,
                )
            })
        }
        MapiObject::Task { folder_id, task_id } => snapshot
            .task_for_id(*folder_id, *task_id)
            .map(|task| (MapiCustomPropertyObjectKind::Task, task.canonical_id)),
        MapiObject::Note { folder_id, note_id } => snapshot
            .note_for_id(*folder_id, *note_id)
            .map(|note| (MapiCustomPropertyObjectKind::Note, note.canonical_id)),
        MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        } => snapshot
            .journal_entry_for_id(*folder_id, *journal_entry_id)
            .map(|entry| {
                (
                    MapiCustomPropertyObjectKind::JournalEntry,
                    entry.canonical_id,
                )
            }),
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| {
                (
                    MapiCustomPropertyObjectKind::Attachment,
                    attachment.canonical_id,
                )
            }),
        MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        } => snapshot
            .public_folder_item_for_id(*folder_id, *item_id)
            .map(|item| (MapiCustomPropertyObjectKind::PublicFolderItem, item.item.id)),
        _ => None,
    }
}

fn staged_custom_property_values(
    object: Option<&MapiObject>,
    property_tags: Option<&[u32]>,
) -> Vec<MapiCustomPropertyValue> {
    let Some(MapiObject::Message {
        pending_properties, ..
    }) = object
    else {
        return Vec::new();
    };
    pending_properties
        .iter()
        .filter(|(tag, _value)| {
            is_custom_property_tag(**tag)
                && property_tags.is_none_or(|property_tags| property_tags.contains(tag))
        })
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, *property_tag, value);
            MapiCustomPropertyValue {
                property_tag: *property_tag,
                property_type: MapiPropertyTag::new(*property_tag).property_type_code(),
                property_value,
            }
        })
        .collect()
}

pub(super) fn is_custom_property_tag(property_tag: u32) -> bool {
    let tag = MapiPropertyTag::new(property_tag);
    tag.property_id() >= FIRST_NAMED_PROPERTY_ID
        && tag.property_type().is_some()
        && !is_canonical_named_property_tag(property_tag)
}

fn is_canonical_named_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_LID_FLAG_REQUEST_W_TAG
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_BUSY_STATUS_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_APPOINTMENT_DURATION_TAG
            | PID_LID_APPOINTMENT_RECUR_TAG
            | PID_LID_APPOINTMENT_SUB_TYPE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_RECURRING_TAG
            | PID_LID_ALL_ATTENDEES_STRING_W_TAG
            | PID_LID_TO_ATTENDEES_STRING_W_TAG
            | PID_LID_CC_ATTENDEES_STRING_W_TAG
            | PID_LID_TIME_ZONE_STRUCT_TAG
            | PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_NOTE_COLOR_TAG
            | PID_LID_LOG_TYPE_W_TAG
            | PID_LID_COMPANIES_TAG
            | PID_LID_CONTACTS_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_VERSION_TAG
            | PID_LID_CONVERSATION_PROCESSED_TAG
            | PID_NAME_KEYWORDS_TAG
    )
}
