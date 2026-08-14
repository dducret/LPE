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

pub(super) fn mapi_event_custom_property_values_from_map(
    properties: &HashMap<u32, MapiValue>,
) -> Vec<MapiEventCustomPropertyValue> {
    let mut values = properties
        .iter()
        .filter(|(tag, _)| is_calendar_passthrough_property_tag(**tag))
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, *property_tag, value);
            MapiEventCustomPropertyValue {
                property_tag: *property_tag,
                property_type: MapiPropertyTag::new(*property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.property_tag);
    values
}

pub(super) fn initial_mapi_event_search_key(
    properties: &HashMap<u32, MapiValue>,
) -> Option<Vec<u8>> {
    match properties.get(&PID_TAG_SEARCH_KEY) {
        Some(MapiValue::Binary(value)) if value.len() == 16 => Some(value.clone()),
        _ => None,
    }
}

pub(super) fn mapi_event_create_property_values_from_map(
    properties: &HashMap<u32, MapiValue>,
    imported_event: bool,
) -> Vec<MapiEventCustomPropertyValue> {
    let mut values = mapi_event_custom_property_values_from_map(properties);
    if imported_event {
        if let Some(search_key) = initial_mapi_event_search_key(properties) {
            let mut property_value = Vec::new();
            write_mapi_value(
                &mut property_value,
                PID_TAG_SEARCH_KEY,
                &MapiValue::Binary(search_key),
            );
            values.push(MapiEventCustomPropertyValue {
                property_tag: PID_TAG_SEARCH_KEY,
                property_type: MapiPropertyType::Binary.as_u16(),
                property_value,
            });
            values.sort_by_key(|value| value.property_tag);
        }
    }
    values
}

pub(super) fn mapi_contact_custom_property_values_from_map(
    properties: &HashMap<u32, MapiValue>,
) -> Vec<MapiContactCustomPropertyValue> {
    let mut values = properties
        .iter()
        .filter(|(tag, _)| is_custom_property_tag(**tag))
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, *property_tag, value);
            MapiContactCustomPropertyValue {
                property_tag: *property_tag,
                property_type: MapiPropertyTag::new(*property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.property_tag);
    values
}

#[cfg(test)]
mod calendar_search_key_tests {
    use super::*;

    #[test]
    fn calendar_create_persists_only_a_16_byte_binary_search_key() {
        let search_key = vec![0x7a; 16];
        let properties =
            HashMap::from([(PID_TAG_SEARCH_KEY, MapiValue::Binary(search_key.clone()))]);
        let values = mapi_event_create_property_values_from_map(&properties, true);
        let stored = values
            .iter()
            .find(|value| value.property_tag == PID_TAG_SEARCH_KEY)
            .expect("valid Calendar SearchKey");
        let mut expected = 16u16.to_le_bytes().to_vec();
        expected.extend_from_slice(&search_key);
        assert_eq!(stored.property_value, expected);

        for invalid in [
            MapiValue::Binary(vec![0x7a; 15]),
            MapiValue::String("invalid".into()),
        ] {
            let values = mapi_event_create_property_values_from_map(
                &HashMap::from([(PID_TAG_SEARCH_KEY, invalid)]),
                true,
            );
            assert!(values
                .iter()
                .all(|value| value.property_tag != PID_TAG_SEARCH_KEY));
        }
        assert!(
            mapi_event_create_property_values_from_map(&properties, false)
                .iter()
                .all(|value| value.property_tag != PID_TAG_SEARCH_KEY)
        );
    }
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
    if let Some(values) =
        effective_local_freebusy_custom_property_values(object, snapshot, Some(property_tags))
    {
        return Ok(values);
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(HashMap::new());
    };
    let requested_tags = property_tags
        .iter()
        .copied()
        .filter_map(|tag| {
            let storage_tag = if object_kind == MapiCustomPropertyObjectKind::CalendarEvent {
                canonical_calendar_property_storage_tag(tag)
            } else {
                tag
            };
            (is_custom_property_tag(storage_tag)
                || (object_kind == MapiCustomPropertyObjectKind::CalendarEvent
                    && is_calendar_passthrough_property_tag(storage_tag)))
            .then_some((tag, storage_tag))
        })
        .collect::<Vec<_>>();
    if requested_tags.is_empty() {
        return Ok(HashMap::new());
    }
    let mut storage_tags = requested_tags
        .iter()
        .map(|(_, storage_tag)| *storage_tag)
        .collect::<Vec<_>>();
    storage_tags.sort_unstable();
    storage_tags.dedup();
    let account_id = custom_property_storage_account_id(principal, object, snapshot);
    let mut stored_values = store
        .fetch_mapi_custom_property_values(account_id, object_kind, canonical_id, &storage_tags)
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value.property_value))
        .collect::<HashMap<_, _>>();
    if let Some(MapiObject::Event { transaction, .. }) = object {
        for tag in &transaction.deleted_properties {
            stored_values.remove(tag);
        }
        for (tag, value) in &transaction.pending_properties {
            if storage_tags.contains(tag) {
                let mut property_value = Vec::new();
                write_mapi_value(&mut property_value, *tag, value);
                stored_values.insert(*tag, property_value);
            }
        }
    }
    let mut values = HashMap::new();
    for (requested_tag, storage_tag) in requested_tags {
        let Some(stored) = stored_values.get(&storage_tag) else {
            continue;
        };
        if requested_tag == storage_tag {
            values.insert(requested_tag, stored.clone());
            continue;
        }
        let mut cursor = Cursor::new(stored);
        let Ok(value) = parse_mapi_property_value(&mut cursor, storage_tag) else {
            continue;
        };
        if cursor.remaining() != 0 {
            continue;
        }
        let mut encoded = Vec::new();
        write_mapi_value(&mut encoded, requested_tag, &value);
        values.insert(requested_tag, encoded);
    }
    Ok(values)
}

pub(super) fn effective_local_freebusy_custom_property_values(
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: Option<&[u32]>,
) -> Option<HashMap<u32, Vec<u8>>> {
    let MapiObject::DelegateFreeBusyMessage {
        folder_id,
        message_id,
        saved_state,
        transaction,
        ..
    } = object?
    else {
        return None;
    };
    let message = snapshot
        .delegate_freebusy_message_for_id(*message_id)
        .filter(|message| message.folder_id == *folder_id)?;
    if !crate::mapi_store::is_outlook_local_freebusy_message(message) {
        return Some(HashMap::new());
    }

    let requested = |tag: u32| property_tags.is_none_or(|tags| tags.contains(&tag));
    let custom_properties = saved_state
        .as_ref()
        .map(|state| state.custom_properties.as_slice())
        .unwrap_or(&message.custom_properties);
    let mut values = custom_properties
        .iter()
        .filter_map(|value| {
            if !requested(value.property_tag) || value.property_type != value.property_tag as u16 {
                return None;
            }
            let mut cursor = Cursor::new(&value.property_value);
            parse_mapi_property_value(&mut cursor, value.property_tag)
                .ok()
                .filter(|_| cursor.remaining() == 0)
                .map(|_| (value.property_tag, value.property_value.clone()))
        })
        .collect::<HashMap<_, _>>();
    for tag in &transaction.deleted_properties {
        values.remove(tag);
    }
    for (tag, value) in &transaction.pending_properties {
        if requested(*tag) {
            let mut encoded = Vec::new();
            write_mapi_value(&mut encoded, *tag, value);
            values.insert(*tag, encoded);
        }
    }
    Some(values)
}

pub(in crate::mapi) fn effective_delegate_freebusy_message(
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<crate::mapi_store::MapiDelegateFreeBusyMessage> {
    let MapiObject::DelegateFreeBusyMessage {
        folder_id,
        message_id,
        saved_state,
        ..
    } = object?
    else {
        return None;
    };
    let mut message = snapshot
        .delegate_freebusy_message_for_id(*message_id)
        .filter(|message| message.folder_id == *folder_id)?
        .clone();
    if let Some(state) = saved_state {
        message.durable_identity = Some(state.identity.clone());
        message.delegates = state.delegates.clone();
        message.custom_properties = state.custom_properties.clone();
    }
    Some(message)
}

async fn custom_property_values_for_copy_source<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: Option<&[u32]>,
) -> Result<Option<HashMap<u32, MapiCustomPropertyValue>>>
where
    S: ExchangeStore,
{
    let Some((source_kind, source_id)) =
        custom_property_object_identity(source, mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    if let Some(values) =
        effective_local_freebusy_custom_property_values(source, snapshot, property_tags)
    {
        return Ok(Some(
            values
                .into_iter()
                .map(|(property_tag, property_value)| {
                    (
                        property_tag,
                        MapiCustomPropertyValue {
                            property_tag,
                            property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                            property_value,
                        },
                    )
                })
                .collect(),
        ));
    }

    let source_account_id = custom_property_storage_account_id(principal, source, snapshot);
    let stored_values = match property_tags {
        Some(property_tags) => {
            store
                .fetch_mapi_custom_property_values(
                    source_account_id,
                    source_kind,
                    source_id,
                    property_tags,
                )
                .await?
        }
        None => {
            store
                .fetch_all_mapi_custom_property_values(source_account_id, source_kind, source_id)
                .await?
        }
    };
    Ok(Some(
        stored_values
            .into_iter()
            .chain(staged_custom_property_values(source, property_tags))
            .map(|value| (value.property_tag, value))
            .collect(),
    ))
}

fn stage_local_freebusy_copied_custom_property_values(
    destination: Option<&mut MapiObject>,
    values: &[MapiCustomPropertyValue],
) -> Result<bool> {
    let Some(MapiObject::DelegateFreeBusyMessage { transaction, .. }) = destination else {
        return Ok(false);
    };
    for value in values {
        let mut cursor = Cursor::new(&value.property_value);
        let property_value = parse_mapi_property_value(&mut cursor, value.property_tag)?;
        if cursor.remaining() != 0 {
            return Err(anyhow!("invalid copied LocalFreebusy property value"));
        }
        transaction.deleted_properties.remove(&value.property_tag);
        transaction
            .pending_properties
            .insert(value.property_tag, property_value);
    }
    Ok(true)
}

pub(super) async fn copy_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    mut destination: Option<&mut MapiObject>,
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
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination.as_deref(), mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    let destination_account_id =
        custom_property_storage_account_id(principal, destination.as_deref(), snapshot);
    let Some(source_values) = custom_property_values_for_copy_source(
        store,
        principal,
        source,
        mailboxes,
        emails,
        snapshot,
        Some(property_tags),
    )
    .await?
    else {
        return Ok(None);
    };
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
        if !stage_local_freebusy_copied_custom_property_values(
            destination.as_deref_mut(),
            &copied_values,
        )? {
            store
                .upsert_mapi_custom_property_values(
                    destination_account_id,
                    destination_kind,
                    destination_id,
                    &copied_values,
                )
                .await?;
        }
    }
    Ok(Some(problems))
}

pub(super) async fn copy_all_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    mut destination: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    excluded_property_tags: &[u32],
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination.as_deref(), mailboxes, emails, snapshot)
    else {
        return Ok(false);
    };
    let destination_account_id =
        custom_property_storage_account_id(principal, destination.as_deref(), snapshot);
    let excluded = excluded_property_tags
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let Some(source_values) = custom_property_values_for_copy_source(
        store, principal, source, mailboxes, emails, snapshot, None,
    )
    .await?
    else {
        return Ok(false);
    };
    let mut values = source_values
        .into_values()
        .filter(|value| {
            is_custom_property_tag(value.property_tag) && !excluded.contains(&value.property_tag)
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.property_tag);
    if values.is_empty() {
        return Ok(false);
    }
    if !stage_local_freebusy_copied_custom_property_values(destination.as_deref_mut(), &values)? {
        store
            .upsert_mapi_custom_property_values(
                destination_account_id,
                destination_kind,
                destination_id,
                &values,
            )
            .await?;
    }
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
    let account_id = custom_property_storage_account_id(principal, object, snapshot);
    store
        .delete_mapi_custom_property_values(account_id, object_kind, canonical_id, &tags)
        .await
}

fn custom_property_storage_account_id(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Uuid {
    match object {
        Some(MapiObject::Event {
            folder_id,
            event_id,
            ..
        }) => snapshot
            .event_for_id(*folder_id, *event_id)
            .map(|event| event.event.owner_account_id)
            .unwrap_or(principal.account_id),
        _ => principal.account_id,
    }
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
        } => saved_email
            .as_ref()
            .map(|saved| &saved.email)
            .or_else(|| message_for_id(*folder_id, *message_id, mailboxes, emails))
            .or_else(|| unique_message_for_id(*message_id, emails))
            .map(|email| (MapiCustomPropertyObjectKind::Message, email.id)),
        MapiObject::Contact {
            folder_id,
            contact_id,
            ..
        } => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| (MapiCustomPropertyObjectKind::Contact, contact.canonical_id)),
        MapiObject::Event {
            folder_id,
            event_id,
            ..
        } => snapshot.event_for_id(*folder_id, *event_id).map(|event| {
            (
                MapiCustomPropertyObjectKind::CalendarEvent,
                event.canonical_id,
            )
        }),
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
        MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
            ..
        } => snapshot
            .delegate_freebusy_message_for_id(*message_id)
            .filter(|message| message.folder_id == *folder_id)
            .filter(|message| crate::mapi_store::is_outlook_local_freebusy_message(message))
            .map(|message| {
                (
                    MapiCustomPropertyObjectKind::DelegateFreeBusyMessage,
                    message.canonical_id,
                )
            }),
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

pub(in crate::mapi) fn is_custom_property_tag(property_tag: u32) -> bool {
    let tag = MapiPropertyTag::new(property_tag);
    tag.property_id() >= MIN_NAMED_PROPERTY_ID
        && tag.property_type().is_some()
        && !is_canonical_named_property_tag(property_tag)
}

pub(in crate::mapi) fn is_calendar_passthrough_property_tag(property_tag: u32) -> bool {
    if is_invalid_calendar_canonical_named_property_tag(property_tag)
        || is_calendar_named_passthrough_property_id(property_tag)
            && !is_calendar_named_passthrough_property_tag(property_tag)
        || crate::store::is_mapi_calendar_standard_passthrough_property_id(property_tag)
            && !is_calendar_standard_passthrough_property_tag(property_tag)
    {
        return false;
    }
    calendar_passthrough_property_type_is_supported(property_tag)
        && (is_custom_property_tag(property_tag)
            || is_calendar_named_passthrough_property_tag(property_tag)
            || is_calendar_standard_passthrough_property_tag(property_tag))
}

pub(in crate::mapi) fn is_unsupported_calendar_passthrough_property_tag(property_tag: u32) -> bool {
    is_invalid_calendar_canonical_named_property_tag(property_tag)
        || is_calendar_named_passthrough_property_id(property_tag)
            && !is_calendar_named_passthrough_property_tag(property_tag)
        || crate::store::is_mapi_calendar_standard_passthrough_property_id(property_tag)
            && !is_calendar_standard_passthrough_property_tag(property_tag)
        || is_custom_property_tag(property_tag)
            && !calendar_passthrough_property_type_is_supported(property_tag)
}

pub(in crate::mapi) fn is_invalid_calendar_canonical_named_property_tag(property_tag: u32) -> bool {
    is_canonical_named_property_id(property_tag) && !is_canonical_named_property_tag(property_tag)
}

fn calendar_passthrough_property_type_is_supported(property_tag: u32) -> bool {
    matches!(
        MapiPropertyTag::new(property_tag).property_type(),
        Some(
            MapiPropertyType::Integer32
                | MapiPropertyType::Boolean
                | MapiPropertyType::Integer64
                | MapiPropertyType::String8
                | MapiPropertyType::String
                | MapiPropertyType::Time
                | MapiPropertyType::Guid
                | MapiPropertyType::ServerId
                | MapiPropertyType::Binary
                | MapiPropertyType::MultipleString8
                | MapiPropertyType::MultipleString
        )
    )
}

fn is_calendar_standard_passthrough_property_tag(property_tag: u32) -> bool {
    crate::store::is_mapi_calendar_standard_passthrough_property_tag(property_tag)
}

fn is_calendar_named_passthrough_property_tag(property_tag: u32) -> bool {
    property_tag == PID_LID_APPOINTMENT_COLOR_TAG
}

pub(in crate::mapi) fn is_calendar_named_passthrough_property_id(property_tag: u32) -> bool {
    property_tag & 0xFFFF_0000 == PID_LID_APPOINTMENT_COLOR_TAG & 0xFFFF_0000
}

fn is_canonical_named_property_tag(property_tag: u32) -> bool {
    if is_exact_canonical_named_property_tag(property_tag) {
        return true;
    }
    let tag = MapiPropertyTag::new(property_tag);
    let unicode_tag = match tag.property_type() {
        Some(MapiPropertyType::String8) => (property_tag & 0xFFFF_0000) | 0x001F,
        Some(MapiPropertyType::MultipleString8) => (property_tag & 0xFFFF_0000) | 0x101F,
        _ => return false,
    };
    is_exact_canonical_named_property_tag(unicode_tag)
}

fn is_canonical_named_property_id(property_tag: u32) -> bool {
    let property_id = property_tag & 0xFFFF_0000;
    [
        0x0002, 0x0003, 0x000B, 0x0014, 0x001F, 0x0040, 0x0048, 0x0102, 0x101F, 0x1102,
    ]
    .into_iter()
    .any(|property_type| is_exact_canonical_named_property_tag(property_id | property_type))
}

fn is_exact_canonical_named_property_tag(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_LID_FLAG_REQUEST_W_TAG
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_TASK_STATUS_TAG
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_TASK_DATE_COMPLETED_TAG
            | PID_LID_TASK_COMPLETE_TAG
            | PID_LID_TASK_F_RECURRING_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_IS_RECURRING_TAG
            | PID_LID_BUSY_STATUS_TAG
            | PID_LID_APPOINTMENT_SEQUENCE_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_CLIP_START_TAG
            | PID_LID_CLIP_END_TAG
            | PID_LID_APPOINTMENT_DURATION_TAG
            | PID_LID_APPOINTMENT_RECUR_TAG
            | PID_LID_APPOINTMENT_SUB_TYPE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_RESPONSE_STATUS_TAG
            | PID_LID_SIDE_EFFECTS_TAG
            | PID_LID_OUTLOOK_COMMON_8578_TAG
            | PID_LID_RECURRING_TAG
            | PID_LID_ALL_ATTENDEES_STRING_W_TAG
            | PID_LID_TO_ATTENDEES_STRING_W_TAG
            | PID_LID_CC_ATTENDEES_STRING_W_TAG
            | PID_LID_TIME_ZONE_STRUCT_TAG
            | PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_DELTA_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_REMINDER_OVERRIDE_TAG
            | PID_LID_REMINDER_PLAY_SOUND_TAG
            | PID_LID_REMINDER_FILE_PARAMETER_W_TAG
            | PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG
            | PID_LID_EMAIL1_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG
            | PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
            | PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
            | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
            | PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
            | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG
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

#[cfg(test)]
mod calendar_passthrough_tests {
    use super::*;

    #[test]
    fn imported_intended_busy_status_preserves_negative_signed_value() {
        let intended_busy_status_tag = 0x8224_0003;
        assert_eq!(
            mapi_event_create_property_values_from_map(
                &HashMap::from([(intended_busy_status_tag, MapiValue::I32(-1))]),
                true,
            ),
            vec![MapiEventCustomPropertyValue {
                property_tag: intended_busy_status_tag,
                property_type: 0x0003,
                property_value: (-1i32).to_le_bytes().to_vec(),
            }]
        );
    }

    #[test]
    fn calendar_named_property_ownership_rejects_alias_types_and_unserializable_values() {
        let location_string8 = (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x001E;
        let location_i32 = (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0003;
        let appointment_color_string = (PID_LID_APPOINTMENT_COLOR_TAG & 0xFFFF_0000) | 0x001F;
        let appointment_color_binary = (PID_LID_APPOINTMENT_COLOR_TAG & 0xFFFF_0000) | 0x0102;
        assert!(!is_custom_property_tag(location_string8));
        assert!(is_custom_property_tag(location_i32));
        assert!(!is_invalid_calendar_canonical_named_property_tag(
            location_string8
        ));
        assert!(is_invalid_calendar_canonical_named_property_tag(
            location_i32
        ));
        assert!(is_custom_property_tag(PID_LID_APPOINTMENT_COLOR_TAG));
        assert!(is_calendar_passthrough_property_tag(
            PID_LID_APPOINTMENT_COLOR_TAG
        ));
        assert_eq!(
            mapi_event_custom_property_values_from_map(&HashMap::from([(
                PID_LID_APPOINTMENT_COLOR_TAG,
                MapiValue::I32(7),
            )])),
            vec![MapiEventCustomPropertyValue {
                property_tag: PID_LID_APPOINTMENT_COLOR_TAG,
                property_type: 0x0003,
                property_value: 7i32.to_le_bytes().to_vec(),
            }]
        );
        for property_tag in [appointment_color_string, appointment_color_binary] {
            assert!(is_custom_property_tag(property_tag));
            assert!(!is_calendar_passthrough_property_tag(property_tag));
            assert!(is_unsupported_calendar_passthrough_property_tag(
                property_tag
            ));
        }
        assert!(is_custom_property_tag(0x9100_1003));
        assert!(is_unsupported_calendar_passthrough_property_tag(
            0x9100_1003
        ));
        assert!(is_unsupported_calendar_passthrough_property_tag(
            0x0063_0003
        ));
        assert!(is_unsupported_calendar_passthrough_property_tag(
            0x0070_0102
        ));
    }
}
