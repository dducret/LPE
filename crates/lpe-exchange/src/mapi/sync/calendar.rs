use super::*;
use crate::mapi::wire::MapiPropertyType;
use lpe_storage::{parse_calendar_participants_metadata, AccessibleEvent};

fn calendar_recipient_sync_fact(
    event: &AccessibleEvent,
    recipient: PendingRecipient,
    track_status: u32,
) -> mapi_mailstore::SpecialMessageRecipientSyncFact {
    let display_name = recipient
        .display_name
        .clone()
        .unwrap_or_else(|| recipient.address.clone());
    let identity = calendar_participant_identity(event, &display_name, &recipient.address);
    mapi_mailstore::SpecialMessageRecipientSyncFact {
        row_id: recipient.row_id,
        recipient_type: u32::from(recipient.recipient_type),
        recipient_flags: recipient.recipient_flags,
        track_status,
        display_type_ex: identity.display_type_ex,
        address_type: identity.address_type,
        email_address: identity.email_address,
        smtp_address: recipient.address,
        display_name,
        entry_id: identity.entry_id,
    }
}

pub(super) fn calendar_recipient_sync_facts(
    event: &AccessibleEvent,
) -> Vec<mapi_mailstore::SpecialMessageRecipientSyncFact> {
    let participant_metadata = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_pending_recipients(event)
        .into_iter()
        .filter(|recipient| !recipient.address.trim().is_empty())
        .map(|recipient| {
            let track_status = if recipient.is_calendar_organizer() {
                0
            } else {
                recipient
                    .row_id
                    .checked_sub(1)
                    .and_then(|index| participant_metadata.attendees.get(index as usize))
                    .map(|attendee| match attendee.partstat.as_str() {
                        "tentative" => 2,
                        "accepted" => 3,
                        "declined" => 4,
                        _ => 0,
                    })
                    .unwrap_or(0)
            };
            calendar_recipient_sync_fact(event, recipient, track_status)
        })
        .collect()
}

pub(super) fn stored_calendar_sync_properties(
    event: &crate::mapi_store::MapiEvent,
) -> Vec<(u32, mapi_mailstore::SpecialMessagePropertyValue)> {
    let mut values = event
        .stored_properties
        .iter()
        .filter(|value| calendar_passthrough_property_is_safe(value.property_tag))
        .filter_map(|value| {
            if value.property_type != value.property_tag as u16 {
                tracing::warn!(
                    event_id = %event.canonical_id,
                    property_tag = format_args!("{:#010X}", value.property_tag),
                    "skipping Calendar property with mismatched stored type"
                );
                return None;
            }
            let mut cursor = Cursor::new(&value.property_value);
            let parsed = match parse_mapi_property_value(&mut cursor, value.property_tag) {
                Ok(parsed) if cursor.remaining() == 0 => parsed,
                Ok(_) | Err(_) => {
                    tracing::warn!(
                        event_id = %event.canonical_id,
                        property_tag = format_args!("{:#010X}", value.property_tag),
                        "skipping invalid stored Calendar property value"
                    );
                    return None;
                }
            };
            let Some(parsed) = special_message_property_value(parsed) else {
                tracing::warn!(
                    event_id = %event.canonical_id,
                    property_tag = format_args!("{:#010X}", value.property_tag),
                    "skipping stored Calendar property type unsupported by full ICS"
                );
                return None;
            };
            Some((
                canonical_calendar_property_storage_tag(value.property_tag),
                parsed,
            ))
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|(tag, _)| {
        (
            MapiPropertyTag::new(*tag).property_id(),
            !matches!(
                MapiPropertyTag::new(*tag).property_type(),
                Some(MapiPropertyType::String | MapiPropertyType::MultipleString)
            ),
        )
    });
    values.dedup_by_key(|(tag, _)| MapiPropertyTag::new(*tag).property_id());
    values
}

fn calendar_passthrough_property_is_safe(property_tag: u32) -> bool {
    let property_id = (property_tag >> 16) as u16;
    !(0x6600..=0x67ff).contains(&property_id)
        && !matches!(
            property_tag,
            PID_TAG_SOURCE_KEY
                | PID_TAG_PARENT_SOURCE_KEY
                | PID_TAG_ENTRY_ID
                | PID_TAG_PARENT_ENTRY_ID
                | PID_TAG_INSTANCE_KEY
                | PID_TAG_RECORD_KEY
                | PID_TAG_SEARCH_KEY
                | PID_TAG_CHANGE_KEY
                | PID_TAG_PREDECESSOR_CHANGE_LIST
                | PID_TAG_CHANGE_NUMBER
                | PID_TAG_ACCESS
                | PID_TAG_ACCESS_LEVEL
                | PID_TAG_HAS_ATTACHMENTS
                | PID_TAG_MESSAGE_STATUS
                | PID_TAG_MESSAGE_FLAGS
        )
        && crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
            property_tag,
        )
}

pub(super) fn calendar_sync_object(
    event: &crate::mapi_store::MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = stored_calendar_sync_properties(event);
    let recipients = calendar_recipient_sync_facts(&event.event);
    for property_tag in [
        PID_TAG_CREATION_TIME,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_LAST_MODIFIER_NAME_W,
        PID_LID_COMMON_START_TAG,
        PID_LID_COMMON_END_TAG,
        PID_LID_BUSY_STATUS_TAG,
        PID_LID_APPOINTMENT_SEQUENCE_TAG,
        PID_LID_LOCATION_W_TAG,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        PID_LID_CLIP_START_TAG,
        PID_LID_CLIP_END_TAG,
        PID_LID_APPOINTMENT_DURATION_TAG,
        PID_LID_APPOINTMENT_SUB_TYPE_TAG,
        PID_LID_APPOINTMENT_RECUR_TAG,
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG,
        PID_LID_RESPONSE_STATUS_TAG,
        PID_LID_SIDE_EFFECTS_TAG,
        PID_LID_OUTLOOK_COMMON_8578_TAG,
        PID_LID_RECURRING_TAG,
        PID_LID_IS_RECURRING_TAG,
        PID_LID_TIME_ZONE_STRUCT_TAG,
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        PID_TAG_HTML_BINARY,
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
        PID_TAG_ICON_INDEX,
        PID_TAG_RTF_IN_SYNC,
        PID_TAG_NATIVE_BODY,
        PID_TAG_SUBJECT_PREFIX_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        PID_LID_TO_ATTENDEES_STRING_W_TAG,
        PID_LID_CC_ATTENDEES_STRING_W_TAG,
        PID_TAG_ACCESS,
        PID_TAG_ACCESS_LEVEL,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_SEARCH_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ] {
        let value = if property_tag == PID_TAG_HAS_ATTACHMENTS {
            Some(mapi_mailstore::SpecialMessagePropertyValue::Bool(
                !event.attachments.is_empty(),
            ))
        } else {
            versioned_event_property_value_with_reminder(event, property_tag, reminder)
                .and_then(special_message_property_value)
        };
        if let Some(value) = value {
            if let Some((_, stored_value)) = properties
                .iter_mut()
                .find(|(stored_tag, _)| *stored_tag == property_tag)
            {
                *stored_value = value;
            } else {
                properties.push((property_tag, value));
            }
        }
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: event.folder_id,
        item_id: event.id,
        canonical_id: event.canonical_id,
        associated: false,
        subject: event.event.title.clone(),
        body_text: Some(crate::mapi::properties::calendar_body_text_for_mapi(
            &event.event,
        )),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(
            &event.version.updated_at,
        ),
        message_size: event_size(&event.event),
        read_state: None,
        recipients,
        named_properties: properties,
        named_property_definitions: HashMap::new(),
    }
}
