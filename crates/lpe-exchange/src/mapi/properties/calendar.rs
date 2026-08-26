mod meeting;
mod projection;

use super::*;
pub(in crate::mapi) use meeting::materialize_owner_meeting_organizer;
use meeting::{appointment_state_flags, organizer_json_from_mapi, response_status};
pub(in crate::mapi) use projection::*;

fn event_mapi_access(event: &AccessibleEvent) -> u32 {
    let mut access = 0;
    if event.rights.may_read {
        access |= MAPI_ACCESS_READ;
    }
    if event.rights.may_write {
        access |= MAPI_ACCESS_MODIFY;
    }
    if event.rights.may_delete {
        access |= MAPI_ACCESS_DELETE;
    }
    access
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

fn calendar_body_html_for_mapi(body_html: &str) -> String {
    let mut projected = String::with_capacity(body_html.len());
    let mut remaining = body_html;
    while let Some(link_offset) = remaining.to_ascii_lowercase().find("<link") {
        projected.push_str(&remaining[..link_offset]);
        let link = &remaining[link_offset..];
        let Some(end_offset) = link.find('>') else {
            projected.push_str(link);
            break;
        };
        let tag = &link[..=end_offset];
        let normalized = tag.to_ascii_lowercase();
        if !(normalized.contains("rel=file-list") || normalized.contains("rel=\"file-list\""))
            || !normalized.contains("href=\"cid:filelist.xml@")
        {
            projected.push_str(tag);
        }
        remaining = &link[end_offset + 1..];
    }
    projected.push_str(remaining);
    projected
}

pub(in crate::mapi) fn calendar_body_text_for_mapi(event: &AccessibleEvent) -> String {
    if event.notes.trim().is_empty() && !event.body_html.trim().is_empty() {
        crate::service::html_to_text(&event.body_html)
    } else {
        event.notes.clone()
    }
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

fn appointment_duration(event: &AccessibleEvent) -> i32 {
    let start = event_start_filetime(event);
    let end = event_end_filetime(event);
    if end <= start {
        return 0;
    }
    ((end - start) / 600_000_000).min(i32::MAX as u64) as i32
}

fn recognized_calendar_time_zone_key(time_zone: &str) -> Option<&'static str> {
    if is_western_europe_calendar_time_zone(time_zone) {
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

fn canonical_calendar_time_zone_key(time_zone: &str) -> Option<&'static str> {
    // [MS-OXOCAL] 2.2.1.41-2.2.1.43 carry the Windows key on the wire;
    // canonical PostgreSQL calendar state uses the corresponding IANA key.
    match recognized_calendar_time_zone_key(time_zone) {
        Some("W. Europe Standard Time") => Some("Europe/Berlin"),
        Some("UTC") => Some("UTC"),
        _ => None,
    }
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
    // [MS-OXCICAL] section 2.1.3.1.1.19: the first TZRULE SHOULD use 1601 as
    // its effective year (and MAY use 1). Outlook uploads 1601 here.
    value.extend_from_slice(&0x0641u16.to_le_bytes());
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

pub(super) fn calendar_global_object_id(event: &AccessibleEvent) -> Vec<u8> {
    calendar_global_object_id_from_uid(&event.uid, event.id)
}

pub(super) fn calendar_clean_global_object_id(event: &AccessibleEvent) -> Vec<u8> {
    calendar_clean_global_object_id_from_uid(&event.uid, event.id)
}

pub(super) fn calendar_global_object_id_from_uid(uid: &str, fallback_id: Uuid) -> Vec<u8> {
    // [MS-OXCICAL] section 2.1.3.1.1.20.26: a native EncodedGlobalId UID
    // is already the hexadecimal GlobalObjectId wire value, but only a full
    // structure with a consistent Size field is safe to project as binary.
    if let Some(value) = lpe_storage::decode_calendar_global_object_id_uid(uid) {
        return value;
    }
    let uid = if uid.is_empty() {
        fallback_id.to_string()
    } else {
        uid.to_string()
    };
    let mut data = b"vCal-Uid".to_vec();
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(uid.as_bytes());

    let mut value = vec![
        0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0,
        0x08,
    ];
    value.extend_from_slice(&[0, 0, 0, 0]);
    // [MS-OXCICAL] section 2.1.3.1.1.20.26 requires a zero Creation Time
    // for a ThirdPartyGlobalId encoded with the vCal-Uid prefix.
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&(data.len().min(u32::MAX as usize) as u32).to_le_bytes());
    value.extend_from_slice(&data);
    value
}

pub(super) fn calendar_clean_global_object_id_from_uid(uid: &str, fallback_id: Uuid) -> Vec<u8> {
    let mut value = calendar_global_object_id_from_uid(uid, fallback_id);
    // [MS-OXOCAL] section 2.2.1.28: CleanGlobalObjectId differs from
    // GlobalObjectId only by zeroing the occurrence Year/Month/Day fields.
    value[16..20].fill(0);
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
        uid: String::new(),
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

// [MS-OXOCAL] section 2.2.1.29: Outlook uses whole minutes since 1601-01-01.
pub(in crate::mapi) fn owner_appointment_id_from_filetime(filetime: u64) -> u32 {
    u32::try_from(filetime / 600_000_000).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_body_html_omits_dangling_outlook_file_list_link() {
        let body = concat!(
            "<html><head><link rel=File-List href=\"cid:filelist.xml@01DD27F4\">",
            "<link rel=stylesheet href=\"calendar.css\"></head><body>Agenda</body></html>"
        );
        let expected = "<html><head><link rel=stylesheet href=\"calendar.css\"></head><body>Agenda</body></html>";

        assert_eq!(calendar_body_html_for_mapi(body), expected);

        let mut event = default_event_for_mapping(Uuid::nil(), "calendar");
        event.body_html = body.to_string();
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_BODY_HTML_W),
            Some(MapiValue::String(expected.to_string()))
        );
    }

    #[test]
    fn calendar_body_text_converts_html_when_plain_text_is_absent() {
        let mut event = default_event_for_mapping(Uuid::nil(), "calendar");
        event.body_html = "<p>Agenda <strong>details</strong></p>".to_string();

        assert_eq!(calendar_body_text_for_mapi(&event), "Agenda details");
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_BODY_W),
            Some(MapiValue::String("Agenda details".to_string()))
        );
    }

    #[test]
    fn calendar_item_access_follows_canonical_grant() {
        let account_id = Uuid::nil();
        let mut read_only = default_event_for_mapping(account_id, "shared-read-only");
        read_only.rights.may_write = false;
        read_only.rights.may_delete = false;
        assert_eq!(
            event_property_value(&read_only, 1, CALENDAR_FOLDER_ID, PID_TAG_ACCESS),
            Some(MapiValue::U32(MAPI_ACCESS_READ))
        );

        let mut writable_delegate = read_only.clone();
        writable_delegate.rights.may_write = true;
        writable_delegate.rights.may_delete = true;
        assert_eq!(
            event_property_value(&writable_delegate, 1, CALENDAR_FOLDER_ID, PID_TAG_ACCESS),
            Some(MapiValue::U32(MAPI_MESSAGE_ACCESS))
        );

        let owner = default_event_for_mapping(account_id, "owned");
        assert_eq!(
            event_property_value(&owner, 1, CALENDAR_FOLDER_ID, PID_TAG_ACCESS),
            Some(MapiValue::U32(MAPI_MESSAGE_ACCESS))
        );
    }

    #[test]
    fn calendar_passthrough_accepts_only_defined_appointment_colors() {
        for color in 0..=10 {
            assert!(validate_calendar_passthrough_invariants(&HashMap::from([(
                PID_LID_APPOINTMENT_COLOR_TAG,
                MapiValue::I32(color),
            )]))
            .is_ok());
        }
        assert!(validate_calendar_passthrough_invariants(&HashMap::from([(
            PID_LID_APPOINTMENT_COLOR_TAG,
            MapiValue::I32(11),
        )]))
        .is_err());
    }

    #[test]
    fn calendar_response_status_distinguishes_organizer_attendee_and_appointment() {
        let mut event = default_event_for_mapping(Uuid::nil(), "calendar");
        event.owner_email = "owner@example.test".to_string();
        event.owner_display_name = "Owner".to_string();
        event.attendees = "Owner".to_string();
        event.organizer_json =
            r#"{"email":"organizer@example.test","common_name":"Organizer","is_meeting":true}"#
                .to_string();

        for (partstat, expected) in [
            ("tentative", 2),
            ("accepted", 3),
            ("declined", 4),
            ("needs-action", 5),
        ] {
            event.attendees_json =
                serialize_calendar_participants_metadata(&CalendarParticipantsMetadata {
                    organizer: Some(CalendarOrganizerMetadata {
                        email: "organizer@example.test".to_string(),
                        common_name: "Organizer".to_string(),
                    }),
                    attendees: vec![CalendarParticipantMetadata {
                        email: "owner@example.test".to_string(),
                        common_name: "Owner".to_string(),
                        role: "REQ-PARTICIPANT".to_string(),
                        partstat: partstat.to_string(),
                        rsvp: false,
                        proposed_start: None,
                        proposed_end: None,
                        counter_proposal: false,
                    }],
                });
            assert_eq!(
                event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_RESPONSE_STATUS_TAG,),
                Some(MapiValue::I32(expected)),
                "attendee participation {partstat}"
            );
        }

        event.organizer_json =
            r#"{"email":"owner@example.test","common_name":"Owner","is_meeting":true}"#.to_string();
        event.attendees_json =
            serialize_calendar_participants_metadata(&CalendarParticipantsMetadata {
                organizer: Some(CalendarOrganizerMetadata {
                    email: "owner@example.test".to_string(),
                    common_name: "Owner".to_string(),
                }),
                attendees: Vec::new(),
            });
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_RESPONSE_STATUS_TAG,),
            Some(MapiValue::I32(1)),
        );

        event.attendees.clear();
        event.attendees_json =
            serialize_calendar_participants_metadata(&CalendarParticipantsMetadata::default());
        event.organizer_json = "{}".to_string();
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_RESPONSE_STATUS_TAG,),
            Some(MapiValue::I32(0)),
        );
    }

    #[test]
    fn imported_response_status_updates_only_the_mailbox_owner_attendee() {
        let mut existing = default_event_for_mapping(Uuid::nil(), "calendar");
        existing.owner_email = "owner@example.test".to_string();
        existing.owner_display_name = "Owner".to_string();
        existing.organizer_json =
            r#"{"email":"organizer@example.test","common_name":"Organizer","is_meeting":true}"#
                .to_string();
        existing.attendees = "Owner, Other".to_string();
        existing.attendees_json =
            serialize_calendar_participants_metadata(&CalendarParticipantsMetadata {
                organizer: Some(CalendarOrganizerMetadata {
                    email: "organizer@example.test".to_string(),
                    common_name: "Organizer".to_string(),
                }),
                attendees: vec![
                    CalendarParticipantMetadata {
                        email: "owner@example.test".to_string(),
                        common_name: "Owner".to_string(),
                        role: "REQ-PARTICIPANT".to_string(),
                        partstat: "needs-action".to_string(),
                        rsvp: true,
                        proposed_start: None,
                        proposed_end: None,
                        counter_proposal: false,
                    },
                    CalendarParticipantMetadata {
                        email: "other@example.test".to_string(),
                        common_name: "Other".to_string(),
                        role: "REQ-PARTICIPANT".to_string(),
                        partstat: "declined".to_string(),
                        rsvp: false,
                        proposed_start: Some("2026-08-25T10:00:00Z".to_string()),
                        proposed_end: Some("2026-08-25T11:00:00Z".to_string()),
                        counter_proposal: true,
                    },
                ],
            });
        let properties = HashMap::from([
            (PID_LID_RESPONSE_STATUS_TAG, MapiValue::I32(3)),
            (
                PID_LID_TO_ATTENDEES_STRING_W_TAG,
                MapiValue::String("Owner; Other".to_string()),
            ),
        ]);

        let mut input =
            event_input_from_mapi(Uuid::nil(), Some(existing.id), &existing, &properties).unwrap();
        apply_calendar_pending_recipients(
            &mut input,
            &existing,
            &properties,
            &[
                PendingRecipient {
                    row_id: 0,
                    recipient_type: 1,
                    recipient_flags: 0x0000_0003,
                    address: "organizer@example.test".to_string(),
                    display_name: Some("Organizer".to_string()),
                },
                PendingRecipient {
                    row_id: 1,
                    recipient_type: 1,
                    recipient_flags: 0x0000_0001,
                    address: "owner@example.test".to_string(),
                    display_name: Some("Owner".to_string()),
                },
                PendingRecipient {
                    row_id: 2,
                    recipient_type: 1,
                    recipient_flags: 0x0000_0001,
                    address: "other@example.test".to_string(),
                    display_name: Some("Other".to_string()),
                },
            ],
        );

        let participants = parse_calendar_participants_metadata(&input.attendees_json);
        let owner = participants
            .attendees
            .iter()
            .find(|attendee| attendee.email == "owner@example.test")
            .unwrap();
        let other = participants
            .attendees
            .iter()
            .find(|attendee| attendee.email == "other@example.test")
            .unwrap();
        assert_eq!(owner.partstat, "accepted");
        assert!(owner.rsvp);
        assert_eq!(other.partstat, "declined");
        assert_eq!(
            other.proposed_start.as_deref(),
            Some("2026-08-25T10:00:00Z")
        );
        assert_eq!(other.proposed_end.as_deref(), Some("2026-08-25T11:00:00Z"));
        assert!(other.counter_proposal);

        let invalid = event_input_from_mapi(
            Uuid::nil(),
            Some(existing.id),
            &existing,
            &HashMap::from([(PID_LID_RESPONSE_STATUS_TAG, MapiValue::I32(6))]),
        )
        .unwrap_err();
        assert!(invalid.to_string().contains("response status 6"));
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
    let imported_uid = calendar_event_uid_from_mapi(properties)?;
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
    let time_zone =
        calendar_time_zone_from_mapi(properties).unwrap_or_else(|| existing.time_zone.clone());
    let start = start_filetime
        .and_then(|filetime| filetime_to_date_time_in_time_zone(filetime, &time_zone))
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let end =
        end_filetime.and_then(|filetime| filetime_to_date_time_in_time_zone(filetime, &time_zone));
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
        uid: imported_uid.unwrap_or_else(|| existing.uid.clone()),
        date,
        time,
        time_zone,
        duration_minutes: end
            .map(|_| duration_minutes)
            .unwrap_or(existing.duration_minutes),
        all_day: properties
            .get(&PID_LID_APPOINTMENT_SUB_TYPE_TAG)
            .and_then(MapiValue::as_bool)
            .unwrap_or(existing.all_day),
        status: calendar_status_from_mapi(properties).unwrap_or_else(|| existing.status.clone()),
        sequence: properties
            .get(&PID_LID_APPOINTMENT_SEQUENCE_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(existing.sequence),
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
        title: clearable_pending_text_property(
            properties,
            &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
            &existing.title,
        ),
        location: clearable_pending_text_property(
            properties,
            &[PID_LID_LOCATION_W_TAG],
            &existing.location,
        ),
        organizer_json: participants.organizer_json,
        attendees: participants.attendees,
        attendees_json: participants.attendees_json,
        notes: clearable_pending_text_property(properties, &[PID_TAG_BODY_W], &existing.notes),
        body_html: clearable_pending_html_property(properties, &existing.body_html),
    })
}

pub(in crate::mapi) fn calendar_event_uid_from_mapi(
    properties: &HashMap<u32, MapiValue>,
) -> Result<Option<String>> {
    calendar_event_uid_from_mapi_with_exception_policy(properties, true)
}

pub(in crate::mapi) fn validate_calendar_event_identity_for_staging(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    calendar_event_uid_from_mapi_with_exception_policy(properties, false).map(|_| ())
}

pub(in crate::mapi) fn validate_calendar_event_input_for_staging(
    account_id: Uuid,
    collection_id: &str,
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    // Retain a structurally valid exception identity on the handle so Save can
    // reject the whole unsupported object instead of creating it without GOID.
    validate_calendar_event_identity_for_staging(properties)?;
    let mut properties = properties.clone();
    for tag in [
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        PID_LID_IS_EXCEPTION_TAG,
    ] {
        properties.remove(&tag);
    }
    event_input_from_mapi(
        account_id,
        None,
        &default_event_for_mapping(account_id, collection_id),
        &properties,
    )
    .map(|_| ())
}

fn calendar_event_uid_from_mapi_with_exception_policy(
    properties: &HashMap<u32, MapiValue>,
    reject_top_level_exception: bool,
) -> Result<Option<String>> {
    let global = match properties.get(&PID_LID_GLOBAL_OBJECT_ID_TAG) {
        Some(MapiValue::Binary(value)) => Some(value.as_slice()),
        Some(_) => bail!("Calendar GlobalObjectId must be binary"),
        None => None,
    };
    let clean = match properties.get(&PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG) {
        Some(MapiValue::Binary(value)) => Some(value.as_slice()),
        Some(_) => bail!("Calendar CleanGlobalObjectId must be binary"),
        None => None,
    };
    let canonical_uid = |value: &[u8]| {
        lpe_storage::calendar_uid_from_global_object_id(value)
            .map(|uid| lpe_storage::normalize_calendar_meeting_uid(&uid))
            .ok_or_else(|| anyhow::anyhow!("Calendar GlobalObjectId is malformed"))
    };
    let global_uid = global.map(canonical_uid).transpose()?;
    let clean_uid = clean.map(canonical_uid).transpose()?;

    if let (Some(global), Some(clean)) = (global, clean) {
        let mut expected_clean = global.to_vec();
        expected_clean[16..20].fill(0);
        if clean != expected_clean {
            bail!("Calendar CleanGlobalObjectId does not match GlobalObjectId");
        }
    }

    let is_exception = match properties.get(&PID_LID_IS_EXCEPTION_TAG) {
        Some(MapiValue::Bool(value)) => *value,
        Some(_) => bail!("Calendar IsException must be boolean"),
        None => false,
    };
    let has_occurrence_date = global.is_some_and(|value| value[16..20] != [0, 0, 0, 0]);
    if reject_top_level_exception && (is_exception || has_occurrence_date) {
        bail!("top-level Calendar exception imports are not supported");
    }

    Ok(global_uid.or(clean_uid))
}

pub(in crate::mapi) fn calendar_pending_recipients(
    event: &AccessibleEvent,
) -> Vec<PendingRecipient> {
    let mut metadata = parse_calendar_participants_metadata(&event.attendees_json);
    let organizer = metadata
        .organizer
        .take()
        .unwrap_or_else(|| calendar_organizer(event));
    let mut recipients = Vec::with_capacity(metadata.attendees.len().saturating_add(1));
    if !organizer.email.is_empty() || !organizer.common_name.is_empty() {
        recipients.push(PendingRecipient {
            row_id: 0,
            recipient_type: 0x01,
            recipient_flags: 0x0000_0003,
            address: organizer.email,
            display_name: (!organizer.common_name.is_empty()).then_some(organizer.common_name),
        });
    }
    recipients.extend(metadata.attendees.into_iter().enumerate().filter_map(
        |(index, attendee)| {
            let recipient_type = match attendee.role.as_str() {
                "OPT-PARTICIPANT" => 0x02,
                "RESOURCE" => 0x03,
                _ => 0x01,
            };
            (!attendee.email.is_empty() || !attendee.common_name.is_empty()).then_some(
                PendingRecipient {
                    row_id: index.saturating_add(1).min(u32::MAX as usize) as u32,
                    recipient_type,
                    recipient_flags: 0x0000_0001,
                    address: attendee.email,
                    display_name: (!attendee.common_name.is_empty())
                        .then_some(attendee.common_name),
                },
            )
        },
    ));
    recipients
}

pub(in crate::mapi) fn apply_calendar_pending_recipients(
    input: &mut UpsertClientEventInput,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) {
    let mut metadata = parse_calendar_participants_metadata(&input.attendees_json);
    let previous_attendees = metadata.attendees.clone();
    if let Some(organizer) = recipients
        .iter()
        .find(|recipient| recipient.is_calendar_organizer())
    {
        metadata.organizer = Some(CalendarOrganizerMetadata {
            email: normalize_calendar_email(&organizer.address),
            common_name: organizer
                .display_name
                .clone()
                .unwrap_or_else(|| organizer.address.clone()),
        });
    }
    metadata.attendees = recipients
        .iter()
        .filter(|recipient| !recipient.is_calendar_organizer())
        .map(|recipient| CalendarParticipantMetadata {
            email: normalize_calendar_email(&recipient.address),
            common_name: recipient
                .display_name
                .clone()
                .unwrap_or_else(|| recipient.address.clone()),
            role: match recipient.recipient_type & 0x0F {
                0x02 => "OPT-PARTICIPANT",
                0x03 => "RESOURCE",
                _ => "REQ-PARTICIPANT",
            }
            .to_string(),
            partstat: "needs-action".to_string(),
            rsvp: false,
            proposed_start: None,
            proposed_end: None,
            counter_proposal: false,
        })
        .collect();
    preserve_calendar_attendee_response_state(&mut metadata.attendees, &previous_attendees);
    apply_owner_response_status_from_mapi(&mut metadata, existing, properties);
    input.attendees = calendar_attendee_labels(&metadata);
    input.attendees_json = serialize_calendar_participants_metadata(&metadata);
    input.organizer_json = organizer_json_from_mapi(
        existing,
        metadata.organizer.as_ref(),
        !metadata.attendees.is_empty(),
        properties,
    );
}

fn clearable_pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
    existing: &str,
) -> String {
    if tags.iter().any(|tag| properties.contains_key(tag)) {
        pending_text_property(properties, tags)
    } else {
        existing.to_string()
    }
}

fn clearable_pending_html_property(properties: &HashMap<u32, MapiValue>, existing: &str) -> String {
    if properties.contains_key(&PID_TAG_BODY_HTML_W) {
        pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
    } else if properties.contains_key(&PID_TAG_HTML_BINARY) {
        pending_html_binary_property(properties).unwrap_or_default()
    } else {
        existing.to_string()
    }
}

fn calendar_time_zone_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    for property_tag in [
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
    ] {
        if let Some(MapiValue::Binary(value)) = properties.get(&property_tag) {
            if let Some(key_name) = calendar_time_zone_definition_key(value) {
                return Some(
                    canonical_calendar_time_zone_key(&key_name)
                        .unwrap_or(key_name.as_str())
                        .to_string(),
                );
            }
        }
    }
    optional_pending_text_property(properties, &[PID_LID_TIME_ZONE_DESCRIPTION_W_TAG])
        .filter(|value| !value.trim().is_empty())
        .map(|description| {
            canonical_calendar_time_zone_key(&description)
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
            proposed_start: None,
            proposed_end: None,
            counter_proposal: false,
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
    let previous_attendees = metadata.attendees.clone();
    if let Some(organizer) = organizer_from_mapi(properties) {
        metadata.organizer = Some(organizer);
    }
    if let Some(attendees) = attendees_from_mapi(properties) {
        metadata.attendees = attendees;
        preserve_calendar_attendee_response_state(&mut metadata.attendees, &previous_attendees);
    }
    apply_owner_response_status_from_mapi(&mut metadata, existing, properties);
    let attendees_json = serialize_calendar_participants_metadata(&metadata);
    let organizer_json = organizer_json_from_mapi(
        existing,
        metadata.organizer.as_ref(),
        !metadata.attendees.is_empty(),
        properties,
    );
    MapiEventParticipants {
        organizer_json,
        attendees: calendar_attendee_labels(&metadata),
        attendees_json,
    }
}

fn preserve_calendar_attendee_response_state(
    attendees: &mut [CalendarParticipantMetadata],
    previous_attendees: &[CalendarParticipantMetadata],
) {
    for attendee in attendees {
        let previous = if attendee.email.trim().is_empty() {
            unique_attendee_with_name(previous_attendees, &attendee.common_name)
        } else {
            let email = normalize_calendar_email(&attendee.email);
            previous_attendees.iter().find(|previous| {
                normalize_calendar_email(&previous.email).eq_ignore_ascii_case(&email)
            })
        };
        let Some(previous) = previous else {
            continue;
        };
        if attendee.email.trim().is_empty() {
            attendee.email = previous.email.clone();
        }
        if attendee.common_name.trim().is_empty() {
            attendee.common_name = previous.common_name.clone();
        }
        attendee.partstat = previous.partstat.clone();
        attendee.rsvp = previous.rsvp;
        attendee.proposed_start = previous.proposed_start.clone();
        attendee.proposed_end = previous.proposed_end.clone();
        attendee.counter_proposal = previous.counter_proposal;
    }
}

fn unique_attendee_with_name<'a>(
    attendees: &'a [CalendarParticipantMetadata],
    common_name: &str,
) -> Option<&'a CalendarParticipantMetadata> {
    let common_name = common_name.trim();
    if common_name.is_empty() {
        return None;
    }
    let mut matching = attendees.iter().filter(|attendee| {
        attendee
            .common_name
            .trim()
            .eq_ignore_ascii_case(common_name)
    });
    let attendee = matching.next()?;
    matching.next().is_none().then_some(attendee)
}

fn apply_owner_response_status_from_mapi(
    metadata: &mut CalendarParticipantsMetadata,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) {
    let partstat = match properties
        .get(&PID_LID_RESPONSE_STATUS_TAG)
        .and_then(MapiValue::as_i64)
    {
        Some(2) => "tentative",
        Some(3) => "accepted",
        Some(4) => "declined",
        Some(5) => "needs-action",
        _ => return,
    };
    let owner_email = normalize_calendar_email(&existing.owner_email);
    if owner_email.is_empty() {
        return;
    }
    let organizer = metadata
        .organizer
        .clone()
        .unwrap_or_else(|| calendar_organizer(existing));
    if normalize_calendar_email(&organizer.email).eq_ignore_ascii_case(&owner_email) {
        return;
    }
    if let Some(attendee) = metadata.attendees.iter_mut().find(|attendee| {
        normalize_calendar_email(&attendee.email).eq_ignore_ascii_case(&owner_email)
    }) {
        attendee.partstat = partstat.to_string();
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
    let required_tags = [
        PID_TAG_DISPLAY_TO_W,
        PID_LID_TO_ATTENDEES_STRING_W_TAG,
        PID_LID_ALL_ATTENDEES_STRING_W_TAG,
    ];
    let optional_tags = [PID_TAG_DISPLAY_CC_W, PID_LID_CC_ATTENDEES_STRING_W_TAG];
    let required = required_tags
        .iter()
        .any(|tag| properties.contains_key(tag))
        .then(|| pending_text_property(properties, &required_tags));
    let optional = optional_tags
        .iter()
        .any(|tag| properties.contains_key(tag))
        .then(|| pending_text_property(properties, &optional_tags));
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
            proposed_start: None,
            proposed_end: None,
            counter_proposal: false,
        })
        .collect()
}

fn calendar_status_from_mapi_busy_status(value: i64) -> String {
    // [MS-OXOCAL] section 2.2.1.2 defines zero as olFree, not cancellation.
    // Cancellation is the asfCanceled bit from section 2.2.1.10.
    match value {
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
    // [MS-OXOCAL] section 2.2.1.39 defines a structured binary value. Do not
    // acknowledge it until that structure can be mapped to canonical IANA state.
    if properties.contains_key(&PID_LID_TIME_ZONE_STRUCT_TAG) {
        return Err(anyhow!(
            "PidLidTimeZoneStruct is not mapped to canonical calendar time-zone state"
        ));
    }
    validate_calendar_passthrough_invariants(properties)?;
    // [MS-OXCMSG] 2.2 permits other Message object properties even when they
    // have no effect on this protocol. Bounded Calendar message properties and
    // named properties that do not map to first-class canonical fields are
    // persisted by the Calendar property-bag helper.
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
        if *tag == PID_LID_RESPONSE_STATUS_TAG {
            let status = value
                .as_i64()
                .ok_or_else(|| anyhow!("invalid MAPI meeting response status value"))?;
            if !(0..=5).contains(&status) {
                return Err(anyhow!("unsupported MAPI meeting response status {status}"));
            }
        }
    }
    Ok(())
}

pub(in crate::mapi) fn validate_calendar_passthrough_invariants(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    if let Some(color) = properties
        .get(&PID_LID_APPOINTMENT_COLOR_TAG)
        .and_then(MapiValue::as_i64)
    {
        // [MS-OXOCAL] section 2.2.1.50 defines the client-authored
        // appointment-color values 0 through 10. Preserve valid values in the
        // bounded Calendar property bag instead of replacing them with zero.
        if !(0..=10).contains(&color) {
            return Err(anyhow!("unsupported MAPI appointment color {color}"));
        }
    }
    let response_requested = properties
        .get(&PID_TAG_RESPONSE_REQUESTED)
        .and_then(MapiValue::as_bool)
        .unwrap_or(false);
    let reply_requested = properties
        .get(&PID_TAG_REPLY_REQUESTED)
        .and_then(MapiValue::as_bool)
        .unwrap_or(false);
    if response_requested != reply_requested {
        return Err(anyhow!(
            "Calendar ResponseRequested and ReplyRequested must agree"
        ));
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
