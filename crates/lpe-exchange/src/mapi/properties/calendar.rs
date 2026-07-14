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
    if let Some(value) = event_reminder_property_value(event, reminder, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(event.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(event.notes.clone())),
        PID_TAG_START_DATE
        | PID_LID_COMMON_START_TAG
        | PID_LID_APPOINTMENT_START_WHOLE_TAG
        | PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME => {
            Some(MapiValue::I64(event_start_filetime(event) as i64))
        }
        PID_TAG_END_DATE | PID_LID_COMMON_END_TAG | PID_LID_APPOINTMENT_END_WHOLE_TAG => {
            Some(MapiValue::I64(event_end_filetime(event) as i64))
        }
        PID_TAG_LOCATION_W | PID_LID_LOCATION_W_TAG => {
            Some(MapiValue::String(event.location.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Appointment".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(event_size(event))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(event_size(event))),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(calendar_organizer_name(event))),
        PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(calendar_organizer_email(event))),
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(calendar_display_to(event))),
        PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(calendar_optional_attendees(event))),
        PID_TAG_BODY_HTML_W => Some(MapiValue::String(event.body_html.clone())),
        PID_TAG_HTML_BINARY => Some(MapiValue::Binary(event.body_html.clone().into_bytes())),
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
        PID_LID_APPOINTMENT_DURATION_TAG => Some(MapiValue::I32(appointment_duration(event))),
        PID_LID_APPOINTMENT_COLOR_TAG => Some(MapiValue::I32(0)),
        PID_LID_SIDE_EFFECTS_TAG => Some(MapiValue::I32(CALENDAR_EVENT_SIDE_EFFECTS)),
        PID_LID_OUTLOOK_COMMON_8578_TAG => Some(MapiValue::I32(0)),
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => Some(MapiValue::Bool(event.all_day)),
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => Some(MapiValue::I32(appointment_state_flags(event))),
        PID_LID_RECURRING_TAG => Some(MapiValue::Bool(!event.recurrence_rule.trim().is_empty())),
        PID_LID_TIME_ZONE_STRUCT_TAG => Some(MapiValue::Binary(calendar_time_zone_struct(event))),
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG => Some(MapiValue::String(
            calendar_time_zone_key(&event.time_zone).to_string(),
        )),
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
        | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG => {
            Some(MapiValue::Binary(calendar_time_zone_definition(event)))
        }
        PID_LID_APPOINTMENT_RECUR_TAG => calendar_recurrence_blob(event).map(MapiValue::Binary),
        PID_LID_GLOBAL_OBJECT_ID_TAG | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG => {
            Some(MapiValue::Binary(calendar_global_object_id(event)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &event.id,
        ))),
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

fn calendar_organizer(event: &AccessibleEvent) -> CalendarOrganizerMetadata {
    parse_calendar_participants_metadata(&event.attendees_json)
        .organizer
        .or_else(|| serde_json::from_str::<CalendarOrganizerMetadata>(&event.organizer_json).ok())
        .unwrap_or_else(|| CalendarOrganizerMetadata {
            email: event.owner_email.clone(),
            common_name: event.owner_display_name.clone(),
        })
}

fn calendar_organizer_name(event: &AccessibleEvent) -> String {
    let organizer = calendar_organizer(event);
    if organizer.common_name.trim().is_empty() {
        organizer.email
    } else {
        organizer.common_name
    }
}

fn calendar_organizer_email(event: &AccessibleEvent) -> String {
    calendar_organizer(event).email
}

fn calendar_display_to(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    let labels = calendar_attendee_labels(&participants);
    if labels.trim().is_empty() {
        event.attendees.clone()
    } else {
        labels
    }
}

fn calendar_all_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(participants.attendees.iter())
}

fn calendar_required_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(
        participants
            .attendees
            .iter()
            .filter(|attendee| !attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    )
}

fn calendar_optional_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(
        participants
            .attendees
            .iter()
            .filter(|attendee| attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    )
}

fn calendar_participant_labels<'a>(
    participants: impl Iterator<Item = &'a CalendarParticipantMetadata>,
) -> String {
    participants
        .map(|attendee| {
            if attendee.common_name.trim().is_empty() {
                attendee.email.trim()
            } else {
                attendee.common_name.trim()
            }
        })
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

fn appointment_busy_status(event: &AccessibleEvent) -> i32 {
    if event.status.eq_ignore_ascii_case("cancelled") {
        0
    } else if event.status.eq_ignore_ascii_case("tentative") {
        1
    } else {
        2
    }
}

fn appointment_state_flags(event: &AccessibleEvent) -> i32 {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    let mut flags = 0;
    if participants.organizer.is_some() || !participants.attendees.is_empty() {
        flags |= 0x0000_0001;
    }
    if event.status.eq_ignore_ascii_case("cancelled") {
        flags |= 0x0000_0004;
    }
    flags
}

fn appointment_duration(event: &AccessibleEvent) -> i32 {
    let start = event_start_filetime(event);
    let end = event_end_filetime(event);
    if end <= start {
        return 0;
    }
    ((end - start) / 600_000_000).min(i32::MAX as u64) as i32
}

fn recognized_calendar_time_zone_key(time_zone: &str) -> Option<&'static str> {
    if time_zone.eq_ignore_ascii_case("W. Europe Standard Time")
        || time_zone.eq_ignore_ascii_case("Europe/Zurich")
        || time_zone.eq_ignore_ascii_case("Europe/Berlin")
        || time_zone.eq_ignore_ascii_case("Europe/Rome")
        || time_zone.eq_ignore_ascii_case("Europe/Vienna")
        || time_zone
            .eq_ignore_ascii_case("(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna")
    {
        Some("W. Europe Standard Time")
    } else if time_zone.eq_ignore_ascii_case("UTC") {
        Some("UTC")
    } else {
        None
    }
}

fn calendar_time_zone_key(time_zone: &str) -> &'static str {
    recognized_calendar_time_zone_key(time_zone).unwrap_or("UTC")
}

fn calendar_time_zone_struct(event: &AccessibleEvent) -> Vec<u8> {
    let tz = calendar_time_zone(event);
    let mut value = Vec::with_capacity(48);
    value.extend_from_slice(&tz.bias.to_le_bytes());
    value.extend_from_slice(&tz.standard_bias.to_le_bytes());
    value.extend_from_slice(&tz.daylight_bias.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    push_system_time(&mut value, tz.standard_date);
    value.extend_from_slice(&0u16.to_le_bytes());
    push_system_time(&mut value, tz.daylight_date);
    value
}

fn calendar_time_zone_definition(event: &AccessibleEvent) -> Vec<u8> {
    let tz = calendar_time_zone(event);
    let key_name = calendar_time_zone_key(&event.time_zone);
    let key_name_units = key_name.encode_utf16().collect::<Vec<_>>();
    let cb_header = 2usize
        .saturating_add(2)
        .saturating_add(key_name_units.len().saturating_mul(2))
        .saturating_add(2)
        .min(u16::MAX as usize) as u16;
    let mut value = Vec::with_capacity(8 + key_name_units.len() * 2 + 66);
    value.push(0x02);
    value.push(0x01);
    value.extend_from_slice(&cb_header.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&(key_name_units.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for unit in key_name_units {
        value.extend_from_slice(&unit.to_le_bytes());
    }
    value.extend_from_slice(&1u16.to_le_bytes());
    push_time_zone_rule(&mut value, tz);
    value
}

#[derive(Clone, Copy)]
struct CalendarTimeZone {
    bias: i32,
    standard_bias: i32,
    daylight_bias: i32,
    standard_date: CalendarSystemTime,
    daylight_date: CalendarSystemTime,
}

#[derive(Clone, Copy)]
struct CalendarSystemTime {
    year: u16,
    month: u16,
    day_of_week: u16,
    day: u16,
    hour: u16,
    minute: u16,
}

fn calendar_time_zone(event: &AccessibleEvent) -> CalendarTimeZone {
    if calendar_time_zone_key(&event.time_zone) == "W. Europe Standard Time" {
        CalendarTimeZone {
            bias: -60,
            standard_bias: 0,
            daylight_bias: -60,
            standard_date: CalendarSystemTime {
                year: 0,
                month: 10,
                day_of_week: 0,
                day: 5,
                hour: 3,
                minute: 0,
            },
            daylight_date: CalendarSystemTime {
                year: 0,
                month: 3,
                day_of_week: 0,
                day: 5,
                hour: 2,
                minute: 0,
            },
        }
    } else {
        CalendarTimeZone {
            bias: 0,
            standard_bias: 0,
            daylight_bias: 0,
            standard_date: CalendarSystemTime::zero(),
            daylight_date: CalendarSystemTime::zero(),
        }
    }
}

impl CalendarSystemTime {
    fn zero() -> Self {
        Self {
            year: 0,
            month: 0,
            day_of_week: 0,
            day: 0,
            hour: 0,
            minute: 0,
        }
    }
}

fn push_time_zone_rule(value: &mut Vec<u8>, tz: CalendarTimeZone) {
    value.push(0x02);
    value.push(0x01);
    value.extend_from_slice(&0x003Eu16.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&[0; 14]);
    value.extend_from_slice(&tz.bias.to_le_bytes());
    value.extend_from_slice(&tz.standard_bias.to_le_bytes());
    value.extend_from_slice(&tz.daylight_bias.to_le_bytes());
    push_system_time(value, tz.standard_date);
    push_system_time(value, tz.daylight_date);
}

fn push_system_time(value: &mut Vec<u8>, system_time: CalendarSystemTime) {
    value.extend_from_slice(&system_time.year.to_le_bytes());
    value.extend_from_slice(&system_time.month.to_le_bytes());
    value.extend_from_slice(&system_time.day_of_week.to_le_bytes());
    value.extend_from_slice(&system_time.day.to_le_bytes());
    value.extend_from_slice(&system_time.hour.to_le_bytes());
    value.extend_from_slice(&system_time.minute.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
}

fn calendar_global_object_id(event: &AccessibleEvent) -> Vec<u8> {
    if let Some(encoded) = event.uid.strip_prefix("mapi-goid:") {
        if let Some(value) = hex_to_bytes(encoded) {
            return value;
        }
    }
    let uid = if event.uid.is_empty() {
        event.id.to_string()
    } else {
        event.uid.clone()
    };
    let mut data = b"vCal-Uid".to_vec();
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(uid.as_bytes());

    let mut value = vec![
        0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0,
        0x08,
    ];
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&(data.len().min(u32::MAX as usize) as u32).to_le_bytes());
    value.extend_from_slice(&data);
    value
}

fn event_reminder_property_value(
    event: &AccessibleEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
    property_tag: u32,
) -> Option<MapiValue> {
    let reminder = reminder?;
    match property_tag {
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(true)),
        PID_LID_REMINDER_DELTA_TAG => Some(MapiValue::I32(reminder_delta_minutes(
            event_start_filetime(event),
            &reminder.reminder_at,
        ))),
        PID_LID_REMINDER_OVERRIDE_TAG | PID_LID_REMINDER_PLAY_SOUND_TAG => {
            Some(MapiValue::Bool(false))
        }
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_REMINDER_SIGNAL_TIME_TAG => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&reminder.reminder_at),
        )),
        PID_LID_REMINDER_TIME_TAG => Some(MapiValue::U64(event_start_filetime(event))),
        _ => None,
    }
}

pub(in crate::mapi) fn default_event_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> AccessibleEvent {
    AccessibleEvent {
        id: Uuid::nil(),
        uid: Uuid::nil().to_string(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: String::new(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
        body_html: String::new(),
    }
}

pub(in crate::mapi) fn default_event_input(
    account_id: Uuid,
    id: Option<Uuid>,
) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id,
        account_id,
        uid: String::new(),
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: String::new(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
        body_html: String::new(),
    }
}

pub(in crate::mapi) fn event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<UpsertClientEventInput> {
    reject_unsupported_mapi_event_properties(properties)?;
    let participants = event_participants_from_mapi(existing, properties);
    let recurrence = properties
        .get(&PID_LID_APPOINTMENT_RECUR_TAG)
        .and_then(|value| match value {
            MapiValue::Binary(value) => Some(appointment_recurrence_from_mapi(value)),
            _ => None,
        })
        .transpose()?;
    let start_filetime = properties
        .get(&PID_TAG_START_DATE)
        .or_else(|| properties.get(&PID_LID_APPOINTMENT_START_WHOLE_TAG))
        .or_else(|| properties.get(&PID_LID_COMMON_START_TAG))
        .and_then(MapiValue::as_i64);
    let end_filetime = properties
        .get(&PID_TAG_END_DATE)
        .or_else(|| properties.get(&PID_LID_APPOINTMENT_END_WHOLE_TAG))
        .or_else(|| properties.get(&PID_LID_COMMON_END_TAG))
        .and_then(MapiValue::as_i64)
        .or_else(|| {
            let start = start_filetime?;
            let duration = properties
                .get(&PID_LID_APPOINTMENT_DURATION_TAG)
                .and_then(MapiValue::as_i64)?;
            Some(start.saturating_add(duration.max(0).saturating_mul(600_000_000)))
        });
    let start = start_filetime
        .and_then(filetime_to_date_time)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let end = end_filetime.and_then(filetime_to_date_time);
    let duration_minutes = match (start_filetime, end_filetime) {
        (Some(start), Some(end)) if end >= start => {
            ((end - start) / 10_000_000 / 60).clamp(0, i64::from(i32::MAX)) as i32
        }
        _ => existing.duration_minutes,
    };
    let (date, time) = start;
    Ok(UpsertClientEventInput {
        id,
        account_id,
        uid: properties
            .get(&PID_LID_GLOBAL_OBJECT_ID_TAG)
            .or_else(|| properties.get(&PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG))
            .and_then(|value| match value {
                MapiValue::Binary(value) => Some(format!(
                    "mapi-goid:{}",
                    lpe_domain::crypto::hex_lower(value)
                )),
                _ => None,
            })
            .unwrap_or_else(|| existing.uid.clone()),
        date,
        time,
        time_zone: calendar_time_zone_from_mapi(properties)
            .unwrap_or_else(|| existing.time_zone.clone()),
        duration_minutes: end
            .map(|_| duration_minutes)
            .unwrap_or(existing.duration_minutes),
        all_day: properties
            .get(&PID_LID_APPOINTMENT_SUB_TYPE_TAG)
            .and_then(MapiValue::as_bool)
            .unwrap_or(existing.all_day),
        status: calendar_status_from_mapi(properties).unwrap_or_else(|| existing.status.clone()),
        sequence: existing.sequence,
        recurrence_rule: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_rule.clone())
            .unwrap_or_else(|| existing.recurrence_rule.clone()),
        recurrence_json: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_json.clone())
            .unwrap_or_else(|| existing.recurrence_json.clone()),
        recurrence_exceptions_json: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_exceptions_json.clone())
            .unwrap_or_else(|| existing.recurrence_exceptions_json.clone()),
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        location: optional_pending_text_property(
            properties,
            &[PID_TAG_LOCATION_W, PID_LID_LOCATION_W_TAG],
        )
        .unwrap_or_else(|| existing.location.clone()),
        organizer_json: participants.organizer_json,
        attendees: participants.attendees,
        attendees_json: participants.attendees_json,
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
        body_html: pending_html_property(properties).unwrap_or_else(|| existing.body_html.clone()),
    })
}

fn calendar_time_zone_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    for property_tag in [
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
    ] {
        if let Some(MapiValue::Binary(value)) = properties.get(&property_tag) {
            if let Some(key_name) = calendar_time_zone_definition_key(value) {
                return Some(
                    recognized_calendar_time_zone_key(&key_name)
                        .unwrap_or(key_name.as_str())
                        .to_string(),
                );
            }
        }
    }
    optional_pending_text_property(properties, &[PID_LID_TIME_ZONE_DESCRIPTION_W_TAG])
        .filter(|value| !value.trim().is_empty())
        .map(|description| {
            recognized_calendar_time_zone_key(&description)
                .unwrap_or(description.as_str())
                .to_string()
        })
}

fn calendar_time_zone_definition_key(value: &[u8]) -> Option<String> {
    // [MS-OXOCAL] 2.2.1.41-2.2.1.43: the display properties contain a
    // little-endian persisted TZDEFINITION whose key name is not null-terminated.
    let major_version = *value.first()?;
    if major_version != 0x02 {
        return None;
    }
    let flags = u16::from_le_bytes(value.get(4..6)?.try_into().ok()?);
    if flags & 0x0002 == 0 {
        return None;
    }
    let mut offset = 6usize;
    if flags & 0x0001 != 0 {
        offset = offset.checked_add(16)?;
    }
    let key_name_length = usize::from(u16::from_le_bytes(
        value.get(offset..offset + 2)?.try_into().ok()?,
    ));
    offset = offset.checked_add(2)?;
    let byte_length = key_name_length.checked_mul(2)?;
    let units = value
        .get(offset..offset.checked_add(byte_length)?)?
        .chunks_exact(2)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        .collect::<Vec<_>>();
    let key_name = String::from_utf16(&units).ok()?;
    (!key_name.trim().is_empty()).then_some(key_name)
}

pub(in crate::mapi) fn meeting_response_event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<Option<UpsertClientEventInput>> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(None);
    };
    let partstat = match message_class.trim().to_ascii_lowercase().as_str() {
        "ipm.schedule.meeting.resp.pos" => "accepted",
        "ipm.schedule.meeting.resp.tent" => "tentative",
        "ipm.schedule.meeting.resp.neg" => "declined",
        _ => return Ok(None),
    };
    for (tag, value) in properties {
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
        let supported = matches!(
            *tag,
            PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_SENDER_NAME_W
                | PID_TAG_SENDER_EMAIL_ADDRESS_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI meeting response property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
    }
    let email = optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
        .map(|value| normalize_calendar_email(&value))
        .unwrap_or_default();
    let common_name = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .unwrap_or_default()
        .trim()
        .to_string();
    if email.is_empty() && common_name.is_empty() {
        bail!("MAPI meeting response requires sender identity");
    }

    let mut metadata = parse_calendar_participants_metadata(&existing.attendees_json);
    let mut matched = false;
    for attendee in &mut metadata.attendees {
        let email_matches = !email.is_empty()
            && normalize_calendar_email(&attendee.email).eq_ignore_ascii_case(&email);
        let name_matches = email.is_empty()
            && !common_name.is_empty()
            && attendee.common_name.eq_ignore_ascii_case(&common_name);
        if email_matches || name_matches {
            attendee.partstat = partstat.to_string();
            matched = true;
        }
    }
    if !matched {
        metadata.attendees.push(CalendarParticipantMetadata {
            email,
            common_name,
            role: "REQ-PARTICIPANT".to_string(),
            partstat: partstat.to_string(),
            rsvp: false,
        });
    }
    let attendees_json = serialize_calendar_participants_metadata(&metadata);
    let attendees = calendar_attendee_labels(&metadata);
    Ok(Some(UpsertClientEventInput {
        id,
        account_id,
        uid: existing.uid.clone(),
        date: existing.date.clone(),
        time: existing.time.clone(),
        time_zone: existing.time_zone.clone(),
        duration_minutes: existing.duration_minutes,
        all_day: existing.all_day,
        status: existing.status.clone(),
        sequence: existing.sequence,
        recurrence_rule: existing.recurrence_rule.clone(),
        recurrence_json: existing.recurrence_json.clone(),
        recurrence_exceptions_json: existing.recurrence_exceptions_json.clone(),
        title: existing.title.clone(),
        location: existing.location.clone(),
        organizer_json: existing.organizer_json.clone(),
        attendees,
        attendees_json,
        notes: existing.notes.clone(),
        body_html: existing.body_html.clone(),
    }))
}

struct MapiEventParticipants {
    organizer_json: String,
    attendees: String,
    attendees_json: String,
}

fn event_participants_from_mapi(
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> MapiEventParticipants {
    let mut metadata = parse_calendar_participants_metadata(&existing.attendees_json);
    if let Some(organizer) = organizer_from_mapi(properties) {
        metadata.organizer = Some(organizer);
    }
    if let Some(attendees) = attendees_from_mapi(properties) {
        metadata.attendees = attendees;
    }
    let attendees_json = serialize_calendar_participants_metadata(&metadata);
    let organizer_json = metadata
        .organizer
        .as_ref()
        .and_then(|organizer| serde_json::to_string(organizer).ok())
        .unwrap_or_else(|| existing.organizer_json.clone());
    MapiEventParticipants {
        organizer_json,
        attendees: calendar_attendee_labels(&metadata),
        attendees_json,
    }
}

fn organizer_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<CalendarOrganizerMetadata> {
    let email = optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
        .map(|value| normalize_calendar_email(&value))
        .unwrap_or_default();
    let common_name = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .unwrap_or_default()
        .trim()
        .to_string();
    (!email.is_empty() || !common_name.is_empty())
        .then_some(CalendarOrganizerMetadata { email, common_name })
}

fn attendees_from_mapi(
    properties: &HashMap<u32, MapiValue>,
) -> Option<Vec<CalendarParticipantMetadata>> {
    let required = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_TO_W,
            PID_LID_TO_ATTENDEES_STRING_W_TAG,
            PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        ],
    );
    let optional = optional_pending_text_property(
        properties,
        &[PID_TAG_DISPLAY_CC_W, PID_LID_CC_ATTENDEES_STRING_W_TAG],
    );
    if required.is_none() && optional.is_none() {
        return None;
    }
    let mut attendees = Vec::new();
    attendees.extend(calendar_participants_from_display_string(
        required.as_deref().unwrap_or_default(),
        "REQ-PARTICIPANT",
    ));
    attendees.extend(calendar_participants_from_display_string(
        optional.as_deref().unwrap_or_default(),
        "OPT-PARTICIPANT",
    ));
    Some(attendees)
}

fn calendar_participants_from_display_string(
    value: &str,
    role: &str,
) -> Vec<CalendarParticipantMetadata> {
    value
        .split([',', ';'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| CalendarParticipantMetadata {
            email: if value.contains('@') {
                normalize_calendar_email(value)
            } else {
                String::new()
            },
            common_name: value.to_string(),
            role: role.to_string(),
            partstat: "needs-action".to_string(),
            rsvp: false,
        })
        .collect()
}

fn calendar_status_from_mapi_busy_status(value: i64) -> String {
    match value {
        0 => "cancelled",
        1 => "tentative",
        _ => "confirmed",
    }
    .to_string()
}

fn calendar_status_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    let state_flags = properties
        .get(&PID_LID_APPOINTMENT_STATE_FLAGS_TAG)
        .and_then(MapiValue::as_i64);
    if state_flags.map(|flags| flags & 0x0000_0004 != 0) == Some(true) {
        return Some("cancelled".to_string());
    }
    properties
        .get(&PID_LID_BUSY_STATUS_TAG)
        .and_then(MapiValue::as_i64)
        .map(calendar_status_from_mapi_busy_status)
}

pub(in crate::mapi) fn reject_unsupported_mapi_event_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    reject_unsupported_calendar_message_class(properties)?;
    // [MS-OXCMSG] 2.2 permits other Message object properties even when they
    // have no effect on this protocol. Calendar named properties that do not
    // map to first-class canonical fields are persisted by the custom-property
    // helper instead of making RopSaveChangesMessage fail.
    for (tag, value) in properties {
        if *tag == PID_LID_APPOINTMENT_STATE_FLAGS_TAG {
            let flags = value
                .as_i64()
                .ok_or_else(|| anyhow!("invalid MAPI appointment state flags value"))?;
            if flags < 0 || flags & !0x0000_0005 != 0 {
                return Err(anyhow!(
                    "unsupported MAPI appointment state flags {flags:#010X}"
                ));
            }
        }
    }
    Ok(())
}

pub(in crate::mapi) fn bounded_meeting_cancellation_from_mapi(
    properties: &HashMap<u32, MapiValue>,
) -> Result<bool> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(false);
    };
    if !message_class
        .trim()
        .eq_ignore_ascii_case("IPM.Schedule.Meeting.Canceled")
    {
        return Ok(false);
    }
    for (tag, value) in properties {
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
        let supported = matches!(
            *tag,
            PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
        );
        if !supported {
            return Err(anyhow!(
                "MAPI calendar cancellation property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
    }
    Ok(true)
}

fn reject_unsupported_calendar_message_class(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(());
    };
    let message_class = message_class.trim();
    if message_class.is_empty()
        || message_class.eq_ignore_ascii_case("IPM.Appointment")
        || message_class.eq_ignore_ascii_case("IPM.Schedule.Meeting.Request")
    {
        return Ok(());
    }
    Err(anyhow!(
        "MAPI calendar message class {message_class} is not mapped to canonical calendar state"
    ))
}

pub(in crate::mapi) async fn apply_canonical_event_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    event_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    if mapi_calendar_event_mutation_suppressed(folder_id, snapshot) {
        bail!("guarded MAPI calendar event mutation is hidden");
    }

    enum EventPropertyMutation {
        None,
        Delete,
        Update(UpsertClientEventInput),
    }

    let event = snapshot
        .event_for_id(folder_id, event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    let (properties, reminder_set, reminder_at) = split_reminder_property_values(values)?;
    let mutation = if properties.is_empty() {
        EventPropertyMutation::None
    } else if bounded_meeting_cancellation_from_mapi(&properties)? {
        EventPropertyMutation::Delete
    } else if let Some(input) = meeting_response_event_input_from_mapi(
        principal.account_id,
        Some(event.canonical_id),
        &event.event,
        &properties,
    )? {
        EventPropertyMutation::Update(input)
    } else {
        EventPropertyMutation::Update(event_input_from_mapi(
            principal.account_id,
            Some(event.canonical_id),
            &event.event,
            &properties,
        )?)
    };
    if matches!(mutation, EventPropertyMutation::Delete) {
        store
            .delete_accessible_event(principal.account_id, event.canonical_id)
            .await?;
        return Ok(());
    }
    if reminder_set.is_some() || reminder_at.is_some() {
        store
            .update_accessible_event_reminder(
                principal.account_id,
                event.canonical_id,
                reminder_set,
                reminder_at,
                None,
            )
            .await?;
    }
    if let EventPropertyMutation::Update(input) = mutation {
        store
            .update_accessible_event(principal.account_id, event.canonical_id, input)
            .await?;
    }
    Ok(())
}

fn mapi_calendar_event_mutation_suppressed(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
}
