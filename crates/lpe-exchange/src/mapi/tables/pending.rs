use super::*;

pub(in crate::mapi) fn serialize_pending_navigation_shortcut_row(
    properties: &HashMap<u32, MapiValue>,
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    let shortcut = navigation_shortcut_from_mapi_properties(principal.account_id, None, properties);
    serialize_navigation_shortcut_row(&shortcut, Some(principal), columns)
}

pub(in crate::mapi) fn navigation_shortcut_from_mapi_properties(
    _account_id: Uuid,
    id: Option<Uuid>,
    properties: &HashMap<u32, MapiValue>,
) -> MapiNavigationShortcutMessage {
    let entry_target = navigation_shortcut_property_by_id(properties, &PID_TAG_WLINK_ENTRY_ID)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => navigation_shortcut_folder_id_from_entry_id(bytes),
            _ => None,
        });
    let subject = properties
        .get(&PID_TAG_SUBJECT_W)
        .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
        .and_then(|value| match value {
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Shortcut".to_string());
    let shortcut_id = id.unwrap_or_else(Uuid::new_v4);
    let shortcut_type = properties
        .get(&PID_TAG_WLINK_TYPE)
        .and_then(MapiValue::as_i64)
        .map(|value| value as u32)
        .unwrap_or(2);
    let group_header_id = navigation_shortcut_property_by_id(
        properties,
        if shortcut_type == 4 {
            &PID_TAG_WLINK_GROUP_HEADER_ID
        } else {
            &PID_TAG_WLINK_GROUP_CLSID
        },
    )
    .and_then(navigation_shortcut_guid_value);
    let group_name = properties
        .get(&PID_TAG_WLINK_GROUP_NAME_W)
        .and_then(|value| match value {
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if shortcut_type == 4 {
                subject.clone()
            } else {
                "Mail".to_string()
            }
        });
    MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapped_mapi_object_id(&shortcut_id)
            .unwrap_or_else(|| crate::mapi::identity::mapi_store_id(0x7fff)),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: shortcut_id,
        subject,
        target_folder_id: entry_target,
        shortcut_type,
        flags: properties
            .get(&PID_TAG_WLINK_FLAGS)
            .and_then(MapiValue::as_i64)
            .map(|value| value as u32)
            .unwrap_or(0),
        save_stamp: properties
            .get(&PID_TAG_WLINK_SAVE_STAMP)
            .and_then(MapiValue::as_i64)
            .map(|value| value as u32)
            .unwrap_or(0),
        section: properties
            .get(&PID_TAG_WLINK_SECTION)
            .and_then(MapiValue::as_i64)
            .map(|value| value as u32)
            .unwrap_or(0),
        ordinal: properties
            .get(&PID_TAG_WLINK_ORDINAL)
            .and_then(|value| match value {
                MapiValue::Binary(bytes) => Some(
                    bytes
                        .iter()
                        .take(4)
                        .fold(0u32, |value, byte| (value << 8) | u32::from(*byte)),
                ),
                _ => None,
            })
            .or_else(|| {
                properties
                    .get(&0x684B_0003)
                    .and_then(MapiValue::as_i64)
                    .map(|value| value as u32)
            })
            .or_else(|| {
                properties
                    .get(&PID_TAG_WLINK_ORDINAL)
                    .and_then(MapiValue::as_i64)
                    .map(|value| value as u32)
            })
            .unwrap_or(0),
        group_header_id,
        group_name,
    }
}

fn navigation_shortcut_folder_id_from_entry_id(bytes: &[u8]) -> Option<u64> {
    crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).or_else(|| {
        bytes
            .windows(46)
            .find_map(crate::mapi::identity::object_id_from_folder_entry_id)
    })
}

fn navigation_shortcut_property_by_id<'a>(
    properties: &'a HashMap<u32, MapiValue>,
    property_tag: &u32,
) -> Option<&'a MapiValue> {
    properties.get(property_tag).or_else(|| {
        properties
            .iter()
            .find(|(tag, _)| property_tag_id_matches(**tag, *property_tag))
            .map(|(_, value)| value)
    })
}

fn navigation_shortcut_guid_value(value: &MapiValue) -> Option<Uuid> {
    match value {
        MapiValue::Guid(value) => Some(Uuid::from_bytes(*value)),
        MapiValue::Binary(value) => value
            .get(..16)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .map(Uuid::from_bytes),
        _ => None,
    }
}

pub(in crate::mapi) fn serialize_pending_note_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let note = note_input_from_mapi(
        principal.account_id,
        None,
        &default_note_for_mapping(),
        properties,
    );
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let note = ClientNote {
        id: Uuid::nil(),
        title: note.title,
        body_text: note.body_text,
        color: note.color,
        categories_json: note.categories_json,
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_note_row(&note, item_id, NOTES_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_journal_entry_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let entry = journal_entry_input_from_mapi(
        principal.account_id,
        None,
        &default_journal_entry_for_mapping(),
        properties,
    );
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let entry = JournalEntry {
        id: Uuid::nil(),
        subject: entry.subject,
        body_text: entry.body_text,
        entry_type: entry.entry_type,
        message_class: entry.message_class,
        starts_at: entry.starts_at,
        ends_at: entry.ends_at,
        occurred_at: entry.occurred_at,
        companies_json: entry.companies_json,
        contacts_json: entry.contacts_json,
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_journal_entry_row(&entry, item_id, JOURNAL_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_conversation_action_row(
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let action = conversation_action_from_mapi_properties(properties);
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let message = MapiConversationActionMessage {
        id: item_id,
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: Uuid::nil(),
        action,
    };
    serialize_conversation_action_row(&message, columns)
}

pub(in crate::mapi) fn conversation_action_from_mapi_properties(
    properties: &HashMap<u32, MapiValue>,
) -> lpe_storage::ConversationAction {
    let conversation_id = properties
        .get(&PID_TAG_CONVERSATION_INDEX)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => conversation_id_from_index(bytes),
            _ => None,
        })
        .unwrap_or_else(Uuid::nil);
    lpe_storage::ConversationAction {
        id: conversation_id,
        conversation_id,
        subject: properties
            .get(&PID_TAG_SUBJECT_W)
            .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
            .and_then(|value| value.as_text())
            .unwrap_or("Conv.Action")
            .to_string(),
        categories_json: match properties.get(&PID_NAME_KEYWORDS_TAG) {
            Some(MapiValue::MultiString(values)) => {
                serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string())
            }
            _ => "[]".to_string(),
        },
        move_folder_entry_id: match properties.get(&PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG)
        {
            Some(MapiValue::Binary(value)) => Some(value.clone()),
            _ => None,
        },
        move_store_entry_id: match properties.get(&PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG) {
            Some(MapiValue::Binary(value)) => Some(value.clone()),
            _ => None,
        },
        move_target_mailbox_id: None,
        max_delivery_time: properties
            .get(&PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_rfc3339_utc),
        last_applied_time: properties
            .get(&PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_rfc3339_utc),
        version: properties
            .get(&PID_LID_CONVERSATION_ACTION_VERSION_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(lpe_storage::CONVERSATION_ACTION_VERSION),
        processed: properties
            .get(&PID_LID_CONVERSATION_PROCESSED_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn serialize_pending_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let lookup_tag = canonical_property_storage_tag(*column);
        let value = match lookup_tag {
            PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(pending_display_recipients(
                recipients, 0x01,
            ))),
            PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(pending_display_recipients(
                recipients, 0x02,
            ))),
            PID_TAG_DISPLAY_BCC_W => Some(MapiValue::String(pending_display_recipients(
                recipients, 0x03,
            ))),
            _ => pending_message_property_value(principal, properties, *column),
        };
        if let Some(value) = value {
            write_mapi_value(&mut row, *column, &value);
        } else {
            write_property_default(&mut row, *column);
        }
    }
    row
}

fn pending_display_recipients(recipients: &[PendingRecipient], recipient_type: u8) -> String {
    // [MS-OXOMSG] 2.2.1.7-2.2.1.9 and [MS-OXPROPS] 2.675, 2.676,
    // and 2.679 define the Bcc, Cc, and To display properties as semicolon-
    // separated PtypString recipient display names.
    recipients
        .iter()
        .filter(|recipient| recipient.recipient_type & 0x0F == recipient_type)
        .map(|recipient| {
            recipient
                .display_name
                .as_deref()
                .filter(|name| !name.is_empty())
                .unwrap_or(&recipient.address)
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(in crate::mapi) fn serialize_pending_associated_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            pending_associated_message_property_value(principal, properties, *column)
        {
            write_mapi_value(&mut row, *column, &value);
        } else {
            write_property_default(&mut row, *column);
        }
    }
    row
}

pub(in crate::mapi) fn pending_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    properties
        .get(&lookup_tag)
        .cloned()
        .or_else(|| match lookup_tag {
            PID_TAG_NORMALIZED_SUBJECT_W => properties.get(&PID_TAG_SUBJECT_W).cloned(),
            PID_TAG_SUBJECT_W => properties.get(&PID_TAG_NORMALIZED_SUBJECT_W).cloned(),
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
            PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
            PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
            PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
            PID_TAG_PRIORITY | PID_TAG_SENSITIVITY => Some(MapiValue::U32(0)),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_UNSENT | MSGFLAG_READ)),
            PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
            PID_TAG_TRUST_SENDER => Some(MapiValue::U32(1)),
            PID_TAG_HAS_NAMED_PROPERTIES => Some(MapiValue::Bool(false)),
            PID_TAG_DISPLAY_BCC_W | PID_TAG_DISPLAY_CC_W | PID_TAG_DISPLAY_TO_W => {
                Some(MapiValue::String(String::new()))
            }
            PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(pending_message_size(properties))),
            PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
                pending_message_size(properties),
            )),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => properties
                .get(&PID_TAG_CREATION_TIME)
                .cloned()
                .or_else(|| properties.get(&PID_TAG_LAST_MODIFICATION_TIME).cloned())
                .or(Some(MapiValue::U64(0))),
            PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(pending_message_change_number(
                    properties,
                )),
            )),
            PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
                mapi_mailstore::predecessor_change_list(pending_message_change_number(properties)),
            )),
            PID_TAG_CHANGE_NUMBER => {
                Some(MapiValue::U64(pending_message_change_number(properties)))
            }
            PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(pending_message_search_key(properties))),
            PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
            PID_TAG_LOCALE_ID => Some(MapiValue::U32(0x0409)),
            PID_TAG_CREATOR_NAME_W | PID_TAG_LAST_MODIFIER_NAME_W => {
                Some(MapiValue::String(principal.display_name.clone()))
            }
            PID_TAG_CREATOR_ENTRY_ID | PID_TAG_LAST_MODIFIER_ENTRY_ID => {
                Some(MapiValue::Binary(mailbox_owner_entry_id(principal)))
            }
            PID_TAG_SENDER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(principal.email.clone())),
            _ => None,
        })
}

pub(in crate::mapi) fn pending_associated_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    // [MS-OXCMSG] 3.2.5.2 gives a newly created message the normal
    // IPM.Note defaults. Configuration properties begin when the client sets them.
    properties
        .get(&lookup_tag)
        .cloned()
        .or_else(|| pending_message_property_value(principal, properties, property_tag))
}

fn pending_message_search_key(properties: &HashMap<u32, MapiValue>) -> Vec<u8> {
    properties
        .get(&PID_TAG_MID)
        .and_then(mapi_value_u64)
        .filter(|message_id| {
            crate::mapi::identity::global_counter_from_store_id(*message_id).is_some()
        })
        .map(mapi_mailstore::source_key_for_store_id)
        .unwrap_or_else(|| {
            mapi_mailstore::change_key_for_change_number(pending_message_change_number(properties))
        })
}

fn pending_message_change_number(properties: &HashMap<u32, MapiValue>) -> u64 {
    properties
        .get(&PID_TAG_CHANGE_NUMBER)
        .and_then(mapi_value_u64)
        .or_else(|| {
            properties
                .get(&PID_TAG_MID)
                .and_then(mapi_value_u64)
                .and_then(crate::mapi::identity::global_counter_from_store_id)
        })
        .or_else(|| {
            properties
                .get(&PID_TAG_SOURCE_KEY)
                .and_then(|value| match value {
                    MapiValue::Binary(value) => {
                        crate::mapi::identity::object_id_from_source_key(value)
                            .and_then(crate::mapi::identity::global_counter_from_store_id)
                    }
                    _ => None,
                })
        })
        .unwrap_or(1)
}

fn mapi_value_u64(value: &MapiValue) -> Option<u64> {
    match value {
        MapiValue::I16(value) => u64::try_from(*value).ok(),
        MapiValue::I32(value) => u64::try_from(*value).ok(),
        MapiValue::I64(value) => u64::try_from(*value).ok(),
        MapiValue::U32(value) => Some(u64::from(*value)),
        MapiValue::U64(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::mapi) fn serialize_pending_contact_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let contact = contact_input_from_mapi(
        principal.account_id,
        None,
        &default_contact_for_mapping(principal.account_id, "default"),
        properties,
    );
    let contact = AccessibleContact {
        id: Uuid::nil(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
        ..Default::default()
    };
    serialize_contact_row(&contact, 0, CONTACTS_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_event_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let event = event_input_from_mapi(
        principal.account_id,
        None,
        &default_event_for_mapping(principal.account_id, "default"),
        properties,
    )
    .unwrap_or_else(|_| default_event_input(principal.account_id, None));
    let event = AccessibleEvent {
        id: Uuid::nil(),
        uid: Uuid::nil().to_string(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        all_day: event.all_day,
        status: event.status,
        sequence: event.sequence,
        recurrence_rule: event.recurrence_rule,
        recurrence_json: event.recurrence_json,
        recurrence_exceptions_json: event.recurrence_exceptions_json,
        title: event.title,
        location: event.location,
        organizer_json: event.organizer_json,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
        body_html: event.body_html,
    };
    serialize_event_row(&event, 0, CALENDAR_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_task_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let task = task_input_from_mapi(
        principal.account_id,
        None,
        &default_task_for_mapping(principal.account_id, "default"),
        Some("default"),
        properties,
    );
    let task = ClientTask {
        id: Uuid::nil(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        is_owned: true,
        rights: default_mapping_rights(),
        task_list_id: task.task_list_id.unwrap_or_else(Uuid::nil),
        task_list_sort_order: 0,
        title: task.title,
        description: task.description,
        status: task.status,
        due_at: task.due_at,
        completed_at: task.completed_at,
        recurrence_rule: task.recurrence_rule,
        sort_order: task.sort_order,
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_task_row(&task, 0, TASKS_FOLDER_ID, columns)
}
