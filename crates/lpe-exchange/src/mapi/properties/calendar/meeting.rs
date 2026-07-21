use super::*;

const ORGANIZER_IS_MEETING_FIELD: &str = "is_meeting";

pub(super) fn appointment_state_flags(event: &AccessibleEvent) -> i32 {
    let mut flags = 0;
    if event_is_meeting(event) {
        flags |= 0x0000_0001;
    }
    if event.status.eq_ignore_ascii_case("cancelled") {
        flags |= 0x0000_0004;
    }
    flags
}

pub(super) fn organizer_json_from_mapi(
    existing: &AccessibleEvent,
    organizer: Option<&CalendarOrganizerMetadata>,
    has_attendees: bool,
    properties: &HashMap<u32, MapiValue>,
) -> String {
    // [MS-OXOCAL] section 3.1.4.2: after an Appointment object becomes a
    // Meeting object, removing every recipient does not convert it back. Keep
    // that user-visible distinction in canonical event JSON, not MAPI session
    // state. Legacy rows without the marker are inferred from their attendees.
    let is_meeting = event_is_meeting(existing)
        || has_attendees
        || properties
            .get(&PID_LID_APPOINTMENT_STATE_FLAGS_TAG)
            .and_then(MapiValue::as_i64)
            .is_some_and(|flags| flags & 0x0000_0001 != 0);

    let mut object = serde_json::from_str::<serde_json::Value>(&existing.organizer_json)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    if let Some(organizer) = organizer {
        object.insert(
            "email".to_string(),
            serde_json::Value::String(organizer.email.clone()),
        );
        object.insert(
            "common_name".to_string(),
            serde_json::Value::String(organizer.common_name.clone()),
        );
    }
    object.insert(
        ORGANIZER_IS_MEETING_FIELD.to_string(),
        serde_json::Value::Bool(is_meeting),
    );
    serde_json::Value::Object(object).to_string()
}

fn event_is_meeting(event: &AccessibleEvent) -> bool {
    let persisted = serde_json::from_str::<serde_json::Value>(&event.organizer_json)
        .ok()
        .and_then(|value| {
            value
                .get(ORGANIZER_IS_MEETING_FIELD)
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false);
    let has_legacy_attendees = !event.attendees.trim().is_empty()
        || !parse_calendar_participants_metadata(&event.attendees_json)
            .attendees
            .is_empty();

    persisted || has_legacy_attendees
}
