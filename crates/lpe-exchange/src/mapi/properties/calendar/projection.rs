use super::*;

pub(in crate::mapi) fn event_property_value(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    event_property_value_with_reminder(event, item_id, folder_id, property_tag, None)
}

pub(in crate::mapi) fn event_property_value_with_reminder(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Option<MapiValue> {
    event_property_value_with_reminder_and_mailbox_guid(
        event,
        item_id,
        folder_id,
        property_tag,
        reminder,
        None,
    )
}

pub(in crate::mapi) fn event_property_value_with_reminder_and_mailbox_guid(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
    mailbox_guid: Option<Uuid>,
) -> Option<MapiValue> {
    event_property_value_with_optional_version(
        event,
        item_id,
        folder_id,
        property_tag,
        reminder,
        None,
        None,
        mailbox_guid,
    )
}

pub(in crate::mapi) fn versioned_event_property_value_with_reminder(
    event: &MapiEvent,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Option<MapiValue> {
    event_property_value_with_optional_version(
        &event.event,
        event.id,
        event.folder_id,
        property_tag,
        reminder,
        Some(&event.version),
        Some(&event.source_key),
        None,
    )
}

fn event_property_value_with_optional_version(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
    version: Option<&lpe_storage::MapiEventVersion>,
    source_key: Option<&[u8]>,
    mailbox_guid: Option<Uuid>,
) -> Option<MapiValue> {
    if let Some(value) = event_reminder_property_value(event, reminder, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_calendar_property_storage_tag(property_tag);
    if property_tag == PID_TAG_SEARCH_KEY {
        return Some(MapiValue::Binary(
            version
                .and_then(|version| version.search_key.clone())
                .unwrap_or_else(|| crate::mapi::identity::generated_message_search_key(&event.id)),
        ));
    }
    if let Some(version) = version {
        match property_tag {
            PID_TAG_CHANGE_KEY => return Some(MapiValue::Binary(version.change_key.clone())),
            PID_TAG_PREDECESSOR_CHANGE_LIST => {
                return Some(MapiValue::Binary(version.predecessor_change_list.clone()))
            }
            PID_TAG_CHANGE_NUMBER => return Some(MapiValue::U64(version.change_number)),
            // [MS-OXOMSG] section 2.2.3.9: MessageDeliveryTime is the
            // server-receipt time. It is not the appointment start time.
            PID_TAG_CREATION_TIME | PID_TAG_MESSAGE_DELIVERY_TIME => {
                return Some(MapiValue::I64(mapi_mailstore::filetime_from_rfc3339_utc(
                    &version.created_at,
                ) as i64))
            }
            PID_TAG_LAST_MODIFICATION_TIME => {
                return Some(MapiValue::I64(version.last_modification_time as i64))
            }
            PID_TAG_LOCAL_COMMIT_TIME => {
                return Some(MapiValue::I64(mapi_mailstore::filetime_from_rfc3339_utc(
                    &version.updated_at,
                ) as i64))
            }
            _ => {}
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(event.title.clone()))
        }
        PID_TAG_SUBJECT_PREFIX_W => Some(MapiValue::String(String::new())),
        PID_TAG_BODY_W => Some(MapiValue::String(calendar_body_text_for_mapi(event))),
        PID_TAG_START_DATE
        | PID_LID_COMMON_START_TAG
        | PID_LID_APPOINTMENT_START_WHOLE_TAG
        | PID_LID_CLIP_START_TAG => Some(MapiValue::I64(event_start_filetime(event) as i64)),
        PID_TAG_END_DATE
        | PID_LID_COMMON_END_TAG
        | PID_LID_APPOINTMENT_END_WHOLE_TAG
        | PID_LID_CLIP_END_TAG => Some(MapiValue::I64(event_end_filetime(event) as i64)),
        PID_TAG_OWNER_APPOINTMENT_ID => Some(MapiValue::U32(owner_appointment_id_from_filetime(
            event_start_filetime(event),
        ))),
        PID_LID_LOCATION_W_TAG => Some(MapiValue::String(event.location.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Appointment".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(event_mapi_access(event))),
        PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(u32::from(event.rights.may_write))),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(event_size(event))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(event_size(event))),
        PID_TAG_LAST_MODIFIER_NAME_W => Some(MapiValue::String(event.owner_display_name.clone())),
        PID_TAG_SENDER_NAME_W | PID_TAG_SENT_REPRESENTING_NAME_W => {
            Some(MapiValue::String(calendar_organizer_name(event)))
        }
        PID_TAG_SENDER_ADDRESS_TYPE_W | PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => Some(
            MapiValue::String(calendar_organizer_identity(event).address_type),
        ),
        PID_TAG_SENDER_EMAIL_ADDRESS_W | PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W => Some(
            MapiValue::String(calendar_organizer_identity(event).email_address),
        ),
        PID_TAG_SENDER_SMTP_ADDRESS_W | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => Some(
            MapiValue::String(calendar_organizer_identity(event).smtp_address),
        ),
        PID_TAG_SENDER_ENTRY_ID | PID_TAG_SENT_REPRESENTING_ENTRY_ID => Some(MapiValue::Binary(
            calendar_organizer_identity(event).entry_id,
        )),
        PID_TAG_SENDER_SEARCH_KEY | PID_TAG_SENT_REPRESENTING_SEARCH_KEY => Some(
            MapiValue::Binary(calendar_organizer_identity(event).search_key),
        ),
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(calendar_display_to(event))),
        PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(calendar_optional_attendees(event))),
        PID_TAG_BODY_HTML_W => Some(MapiValue::String(calendar_body_html_for_mapi(
            &event.body_html,
        ))),
        PID_TAG_HTML_BINARY => Some(MapiValue::Binary(
            calendar_body_html_for_mapi(&event.body_html).into_bytes(),
        )),
        PID_TAG_ICON_INDEX => Some(MapiValue::I32(calendar_icon_index(event))),
        PID_TAG_RTF_IN_SYNC => Some(MapiValue::Bool(false)),
        PID_TAG_NATIVE_BODY => Some(MapiValue::I32(calendar_native_body(event))),
        PID_LID_ALL_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_all_attendees(event)))
        }
        PID_LID_TO_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_required_attendees(event)))
        }
        PID_LID_CC_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_optional_attendees(event)))
        }
        PID_LID_BUSY_STATUS_TAG => Some(MapiValue::I32(appointment_busy_status(event))),
        PID_LID_APPOINTMENT_SEQUENCE_TAG => Some(MapiValue::I32(event.sequence)),
        PID_LID_APPOINTMENT_DURATION_TAG => Some(MapiValue::I32(appointment_duration(event))),
        PID_LID_SIDE_EFFECTS_TAG => Some(MapiValue::I32(CALENDAR_EVENT_SIDE_EFFECTS)),
        PID_LID_OUTLOOK_COMMON_8578_TAG => Some(MapiValue::I32(0)),
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => Some(MapiValue::Bool(event.all_day)),
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => Some(MapiValue::I32(appointment_state_flags(event))),
        PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG => {
            Some(MapiValue::Bool(calendar_counter_proposal_count(event) > 0))
        }
        PID_LID_APPOINTMENT_PROPOSAL_NUMBER_TAG => {
            Some(MapiValue::I32(calendar_counter_proposal_count(event) as i32))
        }
        PID_LID_RESPONSE_STATUS_TAG => Some(MapiValue::I32(response_status(event))),
        PID_LID_RECURRING_TAG => Some(MapiValue::Bool(!event.recurrence_rule.trim().is_empty())),
        PID_LID_IS_RECURRING_TAG => Some(MapiValue::Bool(!event.recurrence_rule.trim().is_empty())),
        PID_LID_TIME_ZONE_STRUCT_TAG => Some(MapiValue::Binary(calendar_time_zone_struct(event))),
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG => Some(MapiValue::String(
            calendar_time_zone_key(&event.time_zone).to_string(),
        )),
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
        | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG => {
            Some(MapiValue::Binary(calendar_time_zone_definition(event)))
        }
        PID_LID_APPOINTMENT_RECUR_TAG => calendar_recurrence_blob(event).map(MapiValue::Binary),
        PID_LID_GLOBAL_OBJECT_ID_TAG => Some(MapiValue::Binary(calendar_global_object_id(event))),
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG => {
            Some(MapiValue::Binary(calendar_clean_global_object_id(event)))
        }
        // [MS-OXCDATA] section 2.2.4.2: a message EntryID is a store
        // provider EntryID, distinct from the eight-byte InstanceKey.
        PID_TAG_ENTRY_ID => mailbox_guid
            .and_then(|mailbox_guid| {
                crate::mapi::identity::message_entry_id_from_object_ids(
                    mailbox_guid,
                    folder_id,
                    item_id,
                )
            })
            .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            source_key
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| mapi_mailstore::source_key_for_store_id(item_id)),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct CalendarParticipantIdentity {
    pub(in crate::mapi) address_type: String,
    pub(in crate::mapi) email_address: String,
    pub(in crate::mapi) smtp_address: String,
    pub(in crate::mapi) entry_id: Vec<u8>,
    pub(in crate::mapi) search_key: Vec<u8>,
    pub(in crate::mapi) display_type_ex: u32,
}

pub(in crate::mapi) fn calendar_participant_identity(
    event: &AccessibleEvent,
    display_name: &str,
    email_address: &str,
) -> CalendarParticipantIdentity {
    if !email_address.trim().is_empty()
        && email_address
            .trim()
            .eq_ignore_ascii_case(event.owner_email.trim())
    {
        let address_book_entry = ExchangeAddressBookEntry {
            id: event.owner_account_id,
            display_name: display_name.to_string(),
            email: email_address.to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: crate::store::ExchangeAddressBookEntryDetails::default(),
        };
        return CalendarParticipantIdentity {
            address_type: "EX".to_string(),
            email_address: crate::mapi::nspi::nspi_entry_unprefixed_legacy_dn(&address_book_entry),
            smtp_address: email_address.to_string(),
            entry_id: crate::mapi::nspi::nspi_entry_permanent_entry_id(&address_book_entry),
            search_key: crate::mapi::nspi::nspi_entry_search_key(&address_book_entry),
            display_type_ex: 0x4000_0000,
        };
    }

    let mut search_key = format!("SMTP:{}", email_address.to_ascii_uppercase()).into_bytes();
    search_key.push(0);
    CalendarParticipantIdentity {
        address_type: "SMTP".to_string(),
        email_address: email_address.to_string(),
        smtp_address: email_address.to_string(),
        entry_id: calendar_one_off_entry_id(display_name, email_address),
        search_key,
        display_type_ex: 0,
    }
}

pub(super) fn calendar_organizer_identity(event: &AccessibleEvent) -> CalendarParticipantIdentity {
    calendar_participant_identity(
        event,
        &calendar_organizer_name(event),
        &calendar_organizer_email(event),
    )
}

pub(in crate::mapi) fn calendar_one_off_entry_id(
    display_name: &str,
    email_address: &str,
) -> Vec<u8> {
    // [MS-OXCDATA] section 2.2.5.1 and [MS-OXCICAL] section
    // 2.1.3.1.1.20.2: unresolved SMTP participants use a Unicode One-Off EntryID.
    let mut entry_id = Vec::new();
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&[
        0x81, 0x2B, 0x1F, 0xA4, 0xBE, 0xA3, 0x10, 0x19, 0x9D, 0x6E, 0x00, 0xDD, 0x01, 0x0F, 0x54,
        0x02,
    ]);
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    entry_id.extend_from_slice(&[0x01, 0x80]);
    for value in [display_name, "SMTP", email_address] {
        for unit in value.encode_utf16() {
            entry_id.extend_from_slice(&unit.to_le_bytes());
        }
        entry_id.extend_from_slice(&0u16.to_le_bytes());
    }
    entry_id
}

pub(super) fn calendar_native_body(event: &AccessibleEvent) -> i32 {
    if !event.body_html.trim().is_empty() {
        3
    } else if !calendar_body_text_for_mapi(event).trim().is_empty() {
        1
    } else {
        0
    }
}

pub(in crate::mapi) fn calendar_icon_index(event: &AccessibleEvent) -> i32 {
    // [MS-OXOCAL] section 2.2.1.49: the low two bits distinguish recurring
    // and meeting objects from a single Appointment object.
    0x0400
        | i32::from(!event.recurrence_rule.trim().is_empty())
        | (i32::from(appointment_state_flags(event) & 0x0000_0001 != 0) << 1)
}

pub(in crate::mapi) fn calendar_enumerable_property_tags(
    event: &MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Vec<u32> {
    let mut tags = vec![
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_SUBJECT_PREFIX_W,
        PID_TAG_BODY_W,
        PID_TAG_HTML_BINARY,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_ACCESS_LEVEL,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_STATUS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_CREATION_TIME,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LAST_MODIFIER_NAME_W,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_ADDRESS_TYPE_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        PID_TAG_SENDER_ENTRY_ID,
        PID_TAG_SENDER_SEARCH_KEY,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_TAG_ICON_INDEX,
        PID_TAG_RTF_IN_SYNC,
        PID_TAG_NATIVE_BODY,
        PID_LID_COMMON_START_TAG,
        PID_LID_COMMON_END_TAG,
        PID_LID_LOCATION_W_TAG,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        PID_LID_CLIP_START_TAG,
        PID_LID_CLIP_END_TAG,
        PID_LID_BUSY_STATUS_TAG,
        PID_LID_APPOINTMENT_SEQUENCE_TAG,
        PID_LID_APPOINTMENT_DURATION_TAG,
        PID_LID_APPOINTMENT_SUB_TYPE_TAG,
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG,
        PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG,
        PID_LID_APPOINTMENT_PROPOSAL_NUMBER_TAG,
        PID_LID_RESPONSE_STATUS_TAG,
        PID_LID_SIDE_EFFECTS_TAG,
        PID_LID_OUTLOOK_COMMON_8578_TAG,
        PID_LID_RECURRING_TAG,
        PID_LID_IS_RECURRING_TAG,
        PID_LID_APPOINTMENT_RECUR_TAG,
        PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        PID_LID_TO_ATTENDEES_STRING_W_TAG,
        PID_LID_CC_ATTENDEES_STRING_W_TAG,
        PID_LID_TIME_ZONE_STRUCT_TAG,
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
        PID_TAG_SEARCH_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ];
    tags.retain(|property_tag| {
        matches!(*property_tag, PID_TAG_ENTRY_ID | PID_TAG_HAS_ATTACHMENTS)
            || versioned_event_property_value_with_reminder(event, *property_tag, reminder)
                .is_some()
    });
    tags
}

fn calendar_counter_proposal_count(event: &AccessibleEvent) -> usize {
    parse_calendar_participants_metadata(&event.attendees_json)
        .attendees
        .iter()
        .filter(|attendee| attendee.counter_proposal)
        .count()
}
