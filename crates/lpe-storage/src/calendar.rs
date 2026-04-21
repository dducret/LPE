use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarParticipantMetadata {
    pub email: String,
    pub common_name: String,
    pub role: String,
    pub partstat: String,
    pub rsvp: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarOrganizerMetadata {
    pub email: String,
    pub common_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarParticipantsMetadata {
    pub organizer: Option<CalendarOrganizerMetadata>,
    pub attendees: Vec<CalendarParticipantMetadata>,
}

pub fn parse_calendar_participants_metadata(raw: &str) -> CalendarParticipantsMetadata {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return CalendarParticipantsMetadata::default();
    }

    if let Ok(metadata) = serde_json::from_str::<CalendarParticipantsMetadata>(trimmed) {
        return normalize_calendar_participants_metadata(metadata);
    }

    if let Ok(attendees) = serde_json::from_str::<Vec<CalendarParticipantMetadata>>(trimmed) {
        return normalize_calendar_participants_metadata(CalendarParticipantsMetadata {
            organizer: None,
            attendees,
        });
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(object) = value.as_object() {
            let mut metadata = CalendarParticipantsMetadata::default();
            for participant in object.values().filter_map(Value::as_object) {
                let email = participant
                    .get("email")
                    .and_then(Value::as_str)
                    .map(normalize_calendar_email)
                    .or_else(|| {
                        participant
                            .get("sendTo")
                            .and_then(Value::as_object)
                            .and_then(|send_to| send_to.get("imip"))
                            .and_then(Value::as_str)
                            .map(normalize_calendar_email)
                    })
                    .unwrap_or_default();
                let common_name = participant
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                let roles = participant.get("roles").and_then(Value::as_object);
                let is_owner = roles
                    .and_then(|roles| roles.get("owner"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_owner && metadata.organizer.is_none() {
                    metadata.organizer = Some(CalendarOrganizerMetadata { email, common_name });
                    continue;
                }
                metadata.attendees.push(CalendarParticipantMetadata {
                    email,
                    common_name,
                    role: if roles
                        .and_then(|roles| roles.get("optional"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        "OPT-PARTICIPANT".to_string()
                    } else {
                        "REQ-PARTICIPANT".to_string()
                    },
                    partstat: normalize_calendar_participation_status(
                        participant
                            .get("participationStatus")
                            .and_then(Value::as_str)
                            .or_else(|| participant.get("partstat").and_then(Value::as_str))
                            .unwrap_or("needs-action"),
                    ),
                    rsvp: participant
                        .get("expectReply")
                        .and_then(Value::as_bool)
                        .or_else(|| participant.get("rsvp").and_then(Value::as_bool))
                        .unwrap_or(false),
                });
            }
            return normalize_calendar_participants_metadata(metadata);
        }
    }

    CalendarParticipantsMetadata::default()
}

pub fn serialize_calendar_participants_metadata(metadata: &CalendarParticipantsMetadata) -> String {
    serde_json::to_string(&normalize_calendar_participants_metadata(metadata.clone()))
        .unwrap_or_else(|_| "{}".to_string())
}

pub fn calendar_attendee_labels(metadata: &CalendarParticipantsMetadata) -> String {
    metadata
        .attendees
        .iter()
        .map(calendar_participant_label)
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn calendar_participant_label(participant: &CalendarParticipantMetadata) -> String {
    if !participant.common_name.trim().is_empty() {
        participant.common_name.trim().to_string()
    } else {
        participant.email.trim().to_string()
    }
}

pub fn normalize_calendar_email(value: &str) -> String {
    value
        .trim()
        .strip_prefix("mailto:")
        .unwrap_or(value.trim())
        .trim()
        .to_ascii_lowercase()
}

pub fn normalize_calendar_participation_status(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" => "accepted".to_string(),
        "declined" => "declined".to_string(),
        "tentative" => "tentative".to_string(),
        "delegated" => "delegated".to_string(),
        _ => "needs-action".to_string(),
    }
}

fn normalize_calendar_participants_metadata(
    mut metadata: CalendarParticipantsMetadata,
) -> CalendarParticipantsMetadata {
    metadata.organizer = metadata.organizer.and_then(|organizer| {
        let email = normalize_calendar_email(&organizer.email);
        let common_name = organizer.common_name.trim().to_string();
        if email.is_empty() && common_name.is_empty() {
            None
        } else {
            Some(CalendarOrganizerMetadata { email, common_name })
        }
    });
    metadata.attendees = metadata
        .attendees
        .into_iter()
        .filter_map(|attendee| {
            let email = normalize_calendar_email(&attendee.email);
            let common_name = attendee.common_name.trim().to_string();
            if email.is_empty() && common_name.is_empty() {
                return None;
            }
            Some(CalendarParticipantMetadata {
                email,
                common_name,
                role: if attendee.role.trim().is_empty() {
                    "REQ-PARTICIPANT".to_string()
                } else {
                    attendee.role.trim().to_ascii_uppercase()
                },
                partstat: normalize_calendar_participation_status(&attendee.partstat),
                rsvp: attendee.rsvp,
            })
        })
        .collect();
    metadata
}
