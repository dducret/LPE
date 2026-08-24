use super::*;

struct EffectiveEventProperties {
    tags: Vec<u32>,
    values_by_property_id: HashMap<u16, (u32, MapiValue)>,
}

// [MS-OXCPRPT] sections 2.2.2, 2.2.3, and 2.2.4 operate on a
// property-bearing Server object. Protocol-control, table, and stream handles
// are live objects, but they are not property sources for these three ROPs.
pub(in crate::mapi) fn object_supports_property_reads(object: &MapiObject) -> bool {
    match object {
        MapiObject::Logon
        | MapiObject::PublicFolderLogon
        | MapiObject::Folder { .. }
        | MapiObject::Message { .. }
        | MapiObject::Contact { .. }
        | MapiObject::Event { .. }
        | MapiObject::Task { .. }
        | MapiObject::Note { .. }
        | MapiObject::JournalEntry { .. }
        | MapiObject::ConversationAction { .. }
        | MapiObject::NavigationShortcut { .. }
        | MapiObject::CommonViewNamedView { .. }
        | MapiObject::SearchFolderDefinitionMessage { .. }
        | MapiObject::AssociatedConfig { .. }
        | MapiObject::DelegateFreeBusyMessage { .. }
        | MapiObject::RecoverableItem { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingMessage { .. }
        | MapiObject::PendingAssociatedMessage { .. }
        | MapiObject::PendingContact { .. }
        | MapiObject::PendingEvent { .. }
        | MapiObject::PendingTask { .. }
        | MapiObject::PendingNote { .. }
        | MapiObject::PendingJournalEntry { .. }
        | MapiObject::PendingConversationAction { .. }
        | MapiObject::PendingNavigationShortcut { .. }
        | MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => true,
        MapiObject::HierarchyTable { .. }
        | MapiObject::ContentsTable { .. }
        | MapiObject::AttachmentTable { .. }
        | MapiObject::PermissionTable { .. }
        | MapiObject::RuleTable { .. }
        | MapiObject::AttachmentStream { .. }
        | MapiObject::NotificationSubscription { .. }
        | MapiObject::SynchronizationSource { .. }
        | MapiObject::SynchronizationCollector { .. }
        | MapiObject::FastTransferDestination { .. } => false,
    }
}

#[cfg(test)]
pub(in crate::mapi) fn rop_get_properties_all_response(
    request: &RopRequest,
    session: &MapiSession,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_get_properties_all_response_with_custom(
        request,
        session,
        object,
        principal,
        mailboxes,
        emails,
        snapshot,
        &HashMap::new(),
    )
}

pub(in crate::mapi) fn rop_get_properties_all_response_with_custom(
    request: &RopRequest,
    session: &MapiSession,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    custom_values: &HashMap<u32, Vec<u8>>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(
            0x08,
            request.input_handle_index().unwrap_or(0),
            MapiError::NullObject.as_u32(),
        );
    };
    if !object_supports_property_reads(object) {
        return rop_error_response(
            0x08,
            request.input_handle_index().unwrap_or(0),
            MapiError::NotSupported.as_u32(),
        );
    }
    if let MapiObject::Event {
        folder_id,
        event_id,
        ..
    } = object
    {
        if snapshot.event_for_id(*folder_id, *event_id).is_none() {
            return rop_error_response(
                0x08,
                request.input_handle_index().unwrap_or(0),
                0x8004_010F,
            );
        }
    }

    let effective_event = effective_event_properties(object, snapshot);
    let message_class = message_enumeration_class(object, snapshot);
    let mut tags = effective_event
        .as_ref()
        .map(|effective| effective.tags.clone())
        .unwrap_or_else(|| get_properties_all_tags(object, snapshot));
    if matches!(object, MapiObject::DelegateFreeBusyMessage { .. }) {
        tags.extend(custom_values.keys().copied());
        tags.sort_unstable();
        tags.dedup();
    }
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let size_limit = request_property_size_limit(request);
    let want_unicode = request_get_properties_all_want_unicode(request);
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for storage_tag in tags {
        let storage_response_tag = get_properties_all_response_tag(storage_tag, want_unicode);
        let response_tag = enumeration_response_tag(
            session,
            effective_event.is_some(),
            message_class,
            storage_response_tag,
        );
        let custom_value = custom_values.get(&storage_tag).map(|encoded| {
            if storage_response_tag == storage_tag {
                return encoded.clone();
            }
            let mut cursor = Cursor::new(encoded);
            let Ok(value) = parse_mapi_property_value(&mut cursor, storage_tag) else {
                return encoded.clone();
            };
            if cursor.remaining() != 0 {
                return encoded.clone();
            }
            let mut converted = Vec::new();
            write_mapi_value(&mut converted, storage_response_tag, &value);
            converted
        });
        let value = custom_value
            .or_else(|| {
                effective_event
                    .as_ref()
                    .and_then(|effective| {
                        effective
                            .values_by_property_id
                            .get(&MapiPropertyTag::new(storage_tag).property_id())
                    })
                    .filter(|(tag, _)| *tag == storage_tag)
                    .map(|(_, value)| {
                        let mut encoded = Vec::new();
                        write_mapi_value(&mut encoded, storage_response_tag, value);
                        encoded
                    })
            })
            .unwrap_or_else(|| {
                serialize_object_property(
                    Some(object),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                    storage_response_tag,
                )
            });
        if size_limit != 0 && value.len() > size_limit {
            write_u32(&mut response, property_error_tag(response_tag));
            write_u32(&mut response, 0x8007_000E);
        } else {
            write_u32(&mut response, response_tag);
            response.extend_from_slice(&value);
        }
    }
    response
}

#[cfg(test)]
pub(in crate::mapi) fn rop_get_properties_list_response(
    request: &RopRequest,
    session: &MapiSession,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_get_properties_list_response_with_custom_tags(request, session, object, snapshot, &[])
}

pub(in crate::mapi) fn rop_get_properties_list_response_with_custom_tags(
    request: &RopRequest,
    session: &MapiSession,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
    custom_property_tags: &[u32],
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(
            0x09,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        );
    };
    if !object_supports_property_reads(object) {
        return rop_error_response(
            0x09,
            request.response_handle_index(),
            MapiError::NotSupported.as_u32(),
        );
    }
    if let MapiObject::Event {
        folder_id,
        event_id,
        ..
    } = object
    {
        if snapshot.event_for_id(*folder_id, *event_id).is_none() {
            return rop_error_response(0x09, request.response_handle_index(), 0x8004_010F);
        }
    }
    let mut tags = effective_event_properties(object, snapshot)
        .map(|effective| effective.tags)
        .unwrap_or_else(|| get_properties_list_tags(object, snapshot));
    if matches!(object, MapiObject::DelegateFreeBusyMessage { .. }) {
        tags.extend_from_slice(custom_property_tags);
        tags.sort_unstable();
        tags.dedup();
    }
    let mut response = vec![0x09, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    let event_enumeration = matches!(
        object,
        MapiObject::Event { .. } | MapiObject::PendingEvent { .. }
    );
    let message_class = message_enumeration_class(object, snapshot);
    for tag in tags {
        write_u32(
            &mut response,
            enumeration_response_tag(session, event_enumeration, message_class, tag),
        );
    }
    response
}

fn enumeration_response_tag(
    session: &MapiSession,
    event_enumeration: bool,
    message_class: Option<&str>,
    property_tag: u32,
) -> u32 {
    let tag = MapiPropertyTag::new(property_tag);
    if (!event_enumeration && message_class.is_none()) || tag.property_id() < MIN_NAMED_PROPERTY_ID
    {
        return property_tag;
    }

    let canonical_definition = if event_enumeration {
        (property_tag == PID_LID_APPOINTMENT_COLOR_TAG
            || !crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
                property_tag,
            ))
        .then(|| fast_transfer_named_property_for_message_tag("IPM.Appointment", property_tag))
        .flatten()
    } else {
        message_class.and_then(|message_class| {
            fast_transfer_named_property_for_message_tag(message_class, property_tag)
        })
    };
    let definition = canonical_definition
        .or_else(|| session.named_property_ids.get(&tag.property_id()).cloned());
    let Some(property_id) = definition
        .as_ref()
        .and_then(|definition| session.named_properties.get(definition))
        .copied()
    else {
        return property_tag;
    };
    (u32::from(property_id) << 16) | u32::from(tag.property_type_code())
}

fn effective_event_properties(
    object: &MapiObject,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<EffectiveEventProperties> {
    let mut values_by_property_id = HashMap::<u16, (u32, MapiValue)>::new();
    let (transaction, stored_values, mut canonical_tags) = match object {
        MapiObject::Event {
            folder_id,
            event_id,
            transaction,
        } => {
            let event = snapshot.event_for_id(*folder_id, *event_id)?;
            (
                Some(transaction),
                event.stored_properties.as_slice(),
                calendar_enumerable_property_tags(
                    event,
                    snapshot.reminder_for_source("calendar", event.canonical_id),
                ),
            )
        }
        MapiObject::PendingEvent { properties, .. } => {
            let canonical_tags = properties
                .keys()
                .copied()
                .map(canonical_calendar_property_storage_tag)
                .filter(|tag| {
                    !crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
                        *tag,
                    ) && !crate::mapi::dispatch::custom_properties::is_invalid_calendar_canonical_named_property_tag(*tag)
                })
                .collect::<Vec<_>>();
            for (tag, value) in properties {
                insert_effective_event_value(&mut values_by_property_id, *tag, value.clone(), true);
            }
            return Some(finalize_effective_event_properties(
                values_by_property_id,
                canonical_tags,
            ));
        }
        _ => return None,
    };

    for stored in stored_values {
        if !crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
            stored.property_tag,
        ) || stored.property_type != stored.property_tag as u16
        {
            continue;
        }
        let mut cursor = Cursor::new(&stored.property_value);
        let Ok(value) = parse_mapi_property_value(&mut cursor, stored.property_tag) else {
            continue;
        };
        if cursor.remaining() != 0 {
            continue;
        }
        insert_effective_event_value(
            &mut values_by_property_id,
            stored.property_tag,
            value,
            false,
        );
    }

    if let Some(transaction) = transaction {
        for tag in &transaction.deleted_properties {
            let property_id = MapiPropertyTag::new(*tag).property_id();
            values_by_property_id.remove(&property_id);
            canonical_tags.retain(|tag| MapiPropertyTag::new(*tag).property_id() != property_id);
        }
        for (tag, value) in &transaction.pending_properties {
            let storage_tag = canonical_calendar_property_storage_tag(*tag);
            if crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
                storage_tag,
            ) {
                insert_effective_event_value(
                    &mut values_by_property_id,
                    storage_tag,
                    value.clone(),
                    true,
                );
            } else if !crate::mapi::dispatch::custom_properties::is_invalid_calendar_canonical_named_property_tag(storage_tag) {
                canonical_tags.push(storage_tag);
            }
        }
    }
    Some(finalize_effective_event_properties(
        values_by_property_id,
        canonical_tags,
    ))
}

fn calendar_enumeration_tag_priority(property_tag: u32) -> u8 {
    if property_tag == PID_TAG_HTML_BINARY {
        0
    } else if matches!(
        MapiPropertyTag::new(property_tag).property_type(),
        Some(MapiPropertyType::String | MapiPropertyType::MultipleString)
    ) {
        1
    } else {
        2
    }
}

fn insert_effective_event_value(
    values: &mut HashMap<u16, (u32, MapiValue)>,
    property_tag: u32,
    value: MapiValue,
    replace: bool,
) {
    if !crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(property_tag)
    {
        return;
    }
    let property_id = MapiPropertyTag::new(property_tag).property_id();
    let unicode = matches!(
        MapiPropertyTag::new(property_tag).property_type(),
        Some(MapiPropertyType::String | MapiPropertyType::MultipleString)
    );
    let replace = match values.get(&property_id) {
        None => true,
        Some((existing_tag, _)) => {
            let existing_unicode = matches!(
                MapiPropertyTag::new(*existing_tag).property_type(),
                Some(MapiPropertyType::String | MapiPropertyType::MultipleString)
            );
            if existing_unicode != unicode {
                unicode
            } else {
                replace
            }
        }
    };
    if replace {
        values.insert(property_id, (property_tag, value));
    }
}

fn finalize_effective_event_properties(
    mut values_by_property_id: HashMap<u16, (u32, MapiValue)>,
    mut tags: Vec<u32>,
) -> EffectiveEventProperties {
    tags.sort_unstable_by_key(|tag| {
        (
            MapiPropertyTag::new(*tag).property_id(),
            calendar_enumeration_tag_priority(*tag),
            *tag,
        )
    });
    tags.dedup_by_key(|tag| MapiPropertyTag::new(*tag).property_id());
    let default_ids = tags
        .iter()
        .map(|tag| MapiPropertyTag::new(*tag).property_id())
        .collect::<HashSet<_>>();
    values_by_property_id.retain(|property_id, _| !default_ids.contains(property_id));
    let mut passthrough_tags = values_by_property_id
        .values()
        .map(|(tag, _)| *tag)
        .collect::<Vec<_>>();
    passthrough_tags.sort_unstable();
    tags.extend(passthrough_tags);
    EffectiveEventProperties {
        tags,
        values_by_property_id,
    }
}

fn get_properties_all_tags(object: &MapiObject, snapshot: &MapiMailStoreSnapshot) -> Vec<u32> {
    match object {
        MapiObject::Logon => default_store_property_tags(),
        MapiObject::PublicFolderLogon => vec![PID_TAG_PRIVATE],
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        } => default_folder_property_tags_with_identity(),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => default_attachment_columns(),
        MapiObject::Message { .. } => message_property_tags(object, snapshot),
        MapiObject::NavigationShortcut { .. }
        | MapiObject::CommonViewNamedView { .. }
        | MapiObject::SearchFolderDefinitionMessage { .. }
        | MapiObject::DelegateFreeBusyMessage { .. }
        | MapiObject::RecoverableItem { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingMessage { .. }
        | MapiObject::PendingAssociatedMessage { .. }
        | MapiObject::PendingNavigationShortcut { .. } => default_message_property_tags(),
        MapiObject::Contact { .. } | MapiObject::PendingContact { .. } => {
            default_contact_property_tags()
        }
        MapiObject::Task { .. } | MapiObject::PendingTask { .. } => default_task_property_tags(),
        MapiObject::Note { .. } | MapiObject::PendingNote { .. } => default_note_property_tags(),
        MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. } => {
            default_journal_entry_property_tags()
        }
        MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. } => {
            default_conversation_action_property_tags()
        }
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
            ..
        } => {
            let mut tags = default_message_property_tags();
            if let Some(message) = saved_message
                .clone()
                .or_else(|| snapshot.associated_config_message_for_id(*config_id))
                .filter(|message| message.folder_id == *folder_id)
            {
                tags.extend(associated_config_named_property_tags(&message));
                tags.sort_unstable();
                tags.dedup();
            }
            tags
        }
        _ => default_folder_property_tags(),
    }
}

fn get_properties_list_tags(object: &MapiObject, snapshot: &MapiMailStoreSnapshot) -> Vec<u32> {
    match object {
        MapiObject::Logon => default_store_property_tags(),
        MapiObject::PublicFolderLogon => vec![PID_TAG_PRIVATE],
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        } => default_folder_property_tags_with_identity(),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => default_attachment_columns(),
        MapiObject::Contact { .. } | MapiObject::PendingContact { .. } => {
            default_contact_property_tags()
        }
        MapiObject::Task { .. } | MapiObject::PendingTask { .. } => default_task_property_tags(),
        MapiObject::Note { .. } | MapiObject::PendingNote { .. } => default_note_property_tags(),
        MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. } => {
            default_journal_entry_property_tags()
        }
        MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. } => {
            default_conversation_action_property_tags()
        }
        MapiObject::Message { .. } => message_property_tags(object, snapshot),
        MapiObject::NavigationShortcut { .. }
        | MapiObject::PendingNavigationShortcut { .. }
        | MapiObject::CommonViewNamedView { .. }
        | MapiObject::SearchFolderDefinitionMessage { .. }
        | MapiObject::AssociatedConfig { .. }
        | MapiObject::DelegateFreeBusyMessage { .. }
        | MapiObject::RecoverableItem { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingAssociatedMessage { .. }
        | MapiObject::PendingMessage { .. } => default_message_property_tags(),
        _ => default_folder_property_tags(),
    }
}

pub(in crate::mapi) fn get_properties_specific_candidate_tags(
    object: Option<&MapiObject>,
) -> Vec<u32> {
    match object {
        Some(MapiObject::Logon) => default_store_property_tags(),
        Some(MapiObject::PublicFolderLogon) => vec![PID_TAG_PRIVATE],
        Some(MapiObject::Contact { .. } | MapiObject::PendingContact { .. }) => {
            default_contact_property_tags()
        }
        Some(MapiObject::Event { .. } | MapiObject::PendingEvent { .. }) => {
            default_event_property_tags()
        }
        Some(MapiObject::Task { .. } | MapiObject::PendingTask { .. }) => {
            default_task_property_tags()
        }
        Some(MapiObject::Note { .. } | MapiObject::PendingNote { .. }) => {
            default_note_property_tags()
        }
        Some(MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. }) => {
            default_journal_entry_property_tags()
        }
        Some(MapiObject::Attachment { .. })
        | Some(MapiObject::PendingAttachment { .. })
        | Some(MapiObject::SavedAttachment { .. }) => default_attachment_columns(),
        Some(MapiObject::Message {
            saved_email: Some(saved_email),
            ..
        }) => message_property_tags_for_email(&saved_email.email),
        Some(
            MapiObject::Message { .. }
            | MapiObject::NavigationShortcut { .. }
            | MapiObject::PendingNavigationShortcut { .. }
            | MapiObject::CommonViewNamedView { .. }
            | MapiObject::SearchFolderDefinitionMessage { .. }
            | MapiObject::AssociatedConfig { .. }
            | MapiObject::DelegateFreeBusyMessage { .. }
            | MapiObject::RecoverableItem { .. }
            | MapiObject::PublicFolderItem { .. }
            | MapiObject::PendingAssociatedMessage { .. }
            | MapiObject::PendingMessage { .. },
        ) => default_message_property_tags(),
        Some(
            MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. },
        ) => default_conversation_action_property_tags(),
        _ => default_folder_property_tags(),
    }
}

fn message_property_tags(object: &MapiObject, snapshot: &MapiMailStoreSnapshot) -> Vec<u32> {
    message_enumeration_email(object, snapshot)
        .map(message_property_tags_for_email)
        .unwrap_or_else(default_message_property_tags)
}

fn message_property_tags_for_email(email: &JmapEmail) -> Vec<u32> {
    let meeting_tags = email_meeting_property_tags(email);
    let mut tags = default_message_property_tags();
    tags.retain(|property_tag| {
        if matches!(
            *property_tag,
            PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_COMMON_END_TAG
        ) {
            meeting_tags.contains(property_tag)
        } else {
            email_property_value(email, *property_tag).is_some()
        }
    });
    tags.extend(email_generated_property_tags(email));
    tags.sort_unstable();
    tags.dedup();
    tags
}

fn message_enumeration_class<'a>(
    object: &'a MapiObject,
    snapshot: &'a MapiMailStoreSnapshot,
) -> Option<&'static str> {
    message_enumeration_email(object, snapshot).map(message_class_for_email)
}

fn message_enumeration_email<'a>(
    object: &'a MapiObject,
    snapshot: &'a MapiMailStoreSnapshot,
) -> Option<&'a JmapEmail> {
    let MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    } = object
    else {
        return None;
    };
    saved_email.as_ref().map(|saved| &saved.email).or_else(|| {
        snapshot
            .message_for_id(*folder_id, *message_id)
            .map(|message| &message.email)
    })
}

fn request_get_properties_all_want_unicode(request: &RopRequest) -> bool {
    request
        .payload
        .get(2..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(1)
        != 0
}

fn get_properties_all_response_tag(property_tag: u32, want_unicode: bool) -> u32 {
    if want_unicode {
        return property_tag;
    }
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::String) => (property_tag & 0xFFFF_0000) | 0x001E,
        Some(MapiPropertyType::MultipleString) => (property_tag & 0xFFFF_0000) | 0x101E,
        _ => property_tag,
    }
}

pub(in crate::mapi) fn property_error_tag(property_tag: u32) -> u32 {
    (property_tag & 0xFFFF_0000) | 0x000A
}
