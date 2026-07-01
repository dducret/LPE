use anyhow::{anyhow, bail, Result};
use lpe_domain::days_from_civil;
use lpe_storage::{
    calendar_attendee_labels, serialize_calendar_participants_metadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata, ContactNameFields,
    JmapEmailFollowupUpdate, UpsertClientContactInput, UpsertClientEventInput,
};
use uuid::Uuid;

use crate::{message::field_text, wbxml::WbxmlNode};
pub(super) fn mail_flag_update(flag: &WbxmlNode) -> Result<JmapEmailFollowupUpdate> {
    if flag.children.is_empty() {
        return Ok(JmapEmailFollowupUpdate {
            flagged: Some(false),
            followup_flag_status: Some("none".to_string()),
            ..Default::default()
        });
    }

    let status = field_text(flag, "Status").unwrap_or_else(|| "0".to_string());
    let mut update = match status.as_str() {
        "0" => JmapEmailFollowupUpdate {
            flagged: Some(false),
            followup_flag_status: Some("none".to_string()),
            ..Default::default()
        },
        "1" => JmapEmailFollowupUpdate {
            flagged: Some(true),
            followup_flag_status: Some("complete".to_string()),
            followup_icon: Some(6),
            todo_item_flags: Some(8),
            ..Default::default()
        },
        "2" => JmapEmailFollowupUpdate {
            flagged: Some(true),
            followup_flag_status: Some("flagged".to_string()),
            followup_icon: Some(6),
            todo_item_flags: Some(8),
            ..Default::default()
        },
        _ => bail!("unsupported ActiveSync mail flag status"),
    };

    if let Some(flag_type) = field_text(flag, "FlagType").filter(|value| !value.is_empty()) {
        update.followup_request = Some(flag_type);
    }
    let start = field_text(flag, "UtcStartDate")
        .or_else(|| field_text(flag, "StartDate"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;
    let due = field_text(flag, "UtcDueDate")
        .or_else(|| field_text(flag, "DueDate"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;
    if start.is_some() != due.is_some() {
        bail!("ActiveSync mail flag start and due dates must be paired");
    }
    update.followup_start_at = start;
    update.followup_due_at = due;
    update.followup_completed_at = field_text(flag, "CompleteTime")
        .or_else(|| field_text(flag, "DateCompleted"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;

    Ok(update)
}

fn active_sync_datetime_to_rfc3339(value: &str) -> Result<String> {
    let value = value.trim();
    if value.contains('-') {
        return Ok(value.to_string());
    }
    let bytes = value.as_bytes();
    if bytes.len() == 16 && bytes[8] == b'T' && bytes[15] == b'Z' {
        return Ok(format!(
            "{}-{}-{}T{}:{}:{}Z",
            &value[0..4],
            &value[4..6],
            &value[6..8],
            &value[9..11],
            &value[11..13],
            &value[13..15]
        ));
    }
    bail!("invalid ActiveSync dateTime value")
}

pub(super) fn parse_contact_input(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: Option<&lpe_storage::ClientContact>,
    application_data: &WbxmlNode,
) -> Result<UpsertClientContactInput> {
    let file_as = field_text(application_data, "FileAs");
    let first_name = field_text(application_data, "FirstName").unwrap_or_default();
    let last_name = field_text(application_data, "LastName").unwrap_or_default();
    let mut structured_name = existing
        .map(|contact| contact.structured_name.clone())
        .unwrap_or_else(ContactNameFields::default);
    if !first_name.is_empty() {
        structured_name.given = first_name.clone();
    }
    if !last_name.is_empty() {
        structured_name.family = last_name.clone();
    }
    let derived_name = format!("{first_name} {last_name}").trim().to_string();
    let name = file_as
        .or_else(|| (!derived_name.is_empty()).then_some(derived_name))
        .or_else(|| existing.map(|contact| contact.name.clone()))
        .unwrap_or_default();
    let email = field_text(application_data, "Email1Address")
        .or_else(|| existing.map(|contact| contact.email.clone()))
        .unwrap_or_default();
    let phone = field_text(application_data, "MobilePhoneNumber")
        .or_else(|| field_text(application_data, "BusinessPhoneNumber"))
        .or_else(|| field_text(application_data, "HomePhoneNumber"))
        .or_else(|| existing.map(|contact| contact.phone.clone()))
        .unwrap_or_default();
    let notes = body_text(application_data)
        .or_else(|| existing.map(|contact| contact.notes.clone()))
        .unwrap_or_default();
    let company_name = field_text(application_data, "CompanyName");
    let job_title =
        field_text(application_data, "JobTitle").or_else(|| field_text(application_data, "Title"));

    Ok(UpsertClientContactInput {
        id,
        account_id,
        name,
        role: job_title
            .clone()
            .or_else(|| existing.map(|contact| contact.role.clone()))
            .unwrap_or_default(),
        email,
        phone,
        team: company_name
            .clone()
            .or_else(|| existing.map(|contact| contact.team.clone()))
            .unwrap_or_default(),
        notes,
        structured_name,
        organization_name: company_name
            .or_else(|| existing.map(|contact| contact.organization_name.clone()))
            .unwrap_or_default(),
        job_title: job_title
            .or_else(|| existing.map(|contact| contact.job_title.clone()))
            .unwrap_or_default(),
        ..Default::default()
    })
}

pub(super) fn parse_event_input(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: Option<&lpe_storage::ClientEvent>,
    application_data: &WbxmlNode,
) -> Result<UpsertClientEventInput> {
    let start = field_text(application_data, "StartTime")
        .or_else(|| {
            existing.map(|event| {
                format!(
                    "{}T{}:00Z",
                    event.date.replace('-', ""),
                    event.time.replace(':', "")
                )
            })
        })
        .unwrap_or_default();
    let (date, time) = parse_compact_datetime(&start)?;
    let end = field_text(application_data, "EndTime");
    let duration_minutes = end
        .as_deref()
        .map(|value| duration_from_datetimes(&start, value))
        .transpose()?
        .or_else(|| existing.map(|event| event.duration_minutes))
        .unwrap_or_default();
    let attendees_metadata = attendees_from_nodes(application_data);
    let attendees = attendees_metadata
        .as_ref()
        .map(calendar_attendee_labels)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| existing.map(|event| event.attendees.clone()))
        .unwrap_or_default();
    let notes = body_text(application_data)
        .or_else(|| existing.map(|event| event.notes.clone()))
        .unwrap_or_default();

    Ok(UpsertClientEventInput {
        id,
        account_id,
        uid: field_text(application_data, "UID")
            .or_else(|| existing.map(|event| event.uid.clone()))
            .unwrap_or_default(),
        date,
        time,
        time_zone: field_text(application_data, "TimeZone")
            .or_else(|| existing.map(|event| event.time_zone.clone()))
            .unwrap_or_default(),
        duration_minutes,
        all_day: field_text(application_data, "AllDayEvent")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .or_else(|| existing.map(|event| event.all_day))
            .unwrap_or(false),
        status: existing
            .map(|event| event.status.clone())
            .unwrap_or_else(|| "confirmed".to_string()),
        sequence: existing.map(|event| event.sequence).unwrap_or(0),
        recurrence_rule: if let Some(recurrence) = application_data.child("Recurrence") {
            if recurrence.children.is_empty() {
                String::new()
            } else {
                recurrence_to_rrule(recurrence)?
            }
        } else {
            existing
                .map(|event| event.recurrence_rule.clone())
                .unwrap_or_default()
        },
        recurrence_json: existing
            .map(|event| event.recurrence_json.clone())
            .unwrap_or_else(|| "{}".to_string()),
        recurrence_exceptions_json: existing
            .map(|event| event.recurrence_exceptions_json.clone())
            .unwrap_or_else(|| "[]".to_string()),
        title: field_text(application_data, "Subject")
            .or_else(|| existing.map(|event| event.title.clone()))
            .unwrap_or_default(),
        location: field_text(application_data, "Location")
            .or_else(|| existing.map(|event| event.location.clone()))
            .unwrap_or_default(),
        organizer_json: existing
            .map(|event| event.organizer_json.clone())
            .unwrap_or_else(|| "{}".to_string()),
        attendees,
        attendees_json: attendees_metadata
            .as_ref()
            .map(serialize_calendar_participants_metadata)
            .or_else(|| existing.map(|event| event.attendees_json.clone()))
            .unwrap_or_default(),
        notes,
        body_html: existing
            .map(|event| event.body_html.clone())
            .unwrap_or_default(),
    })
}

fn body_text(application_data: &WbxmlNode) -> Option<String> {
    application_data.child("Body").and_then(|body| {
        body.child("Data")
            .map(|node| node.text_value().trim().to_string())
            .or_else(|| {
                let value = body.text_value().trim();
                (!value.is_empty()).then(|| value.to_string())
            })
    })
}

fn parse_compact_datetime(value: &str) -> Result<(String, String)> {
    let compact = value.trim().trim_end_matches('Z');
    let (date_part, time_part) = compact
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid ActiveSync datetime"))?;
    if date_part.len() != 8 || time_part.len() < 4 {
        bail!("invalid ActiveSync datetime");
    }
    Ok((
        format!(
            "{}-{}-{}",
            &date_part[0..4],
            &date_part[4..6],
            &date_part[6..8]
        ),
        format!("{}:{}", &time_part[0..2], &time_part[2..4]),
    ))
}

fn duration_from_datetimes(start: &str, end: &str) -> Result<i32> {
    let (start_date, start_time) = parse_compact_datetime(start)?;
    let (end_date, end_time) = parse_compact_datetime(end)?;
    let start_minutes = date_time_to_minutes(&start_date, &start_time)?;
    let end_minutes = date_time_to_minutes(&end_date, &end_time)?;
    Ok((end_minutes - start_minutes).max(0) as i32)
}

fn date_time_to_minutes(date: &str, time: &str) -> Result<i64> {
    let mut date_parts = date.split('-');
    let year = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let month = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let day = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let mut time_parts = time.split(':');
    let hour = time_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync time"))?
        .parse::<i64>()?;
    let minute = time_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync time"))?
        .parse::<i64>()?;
    Ok(days_from_civil(year, month, day) * 1440 + hour * 60 + minute)
}

fn attendees_from_nodes(application_data: &WbxmlNode) -> Option<CalendarParticipantsMetadata> {
    let attendees_node = application_data.child("Attendees")?;
    let attendees = attendees_node
        .children_named("Attendee")
        .into_iter()
        .filter_map(|attendee| {
            let email = attendee
                .child("Email")
                .map(|value| value.text_value().trim())
                .unwrap_or("");
            let name = attendee
                .child("Name")
                .map(|value| value.text_value().trim())
                .unwrap_or("");
            if name.is_empty() && email.is_empty() {
                return None;
            }
            Some(CalendarParticipantMetadata {
                email: email.to_ascii_lowercase(),
                common_name: name.to_string(),
                role: match attendee
                    .child("AttendeeType")
                    .map(|node| node.text_value().trim())
                {
                    Some("2") => "OPT-PARTICIPANT".to_string(),
                    _ => "REQ-PARTICIPANT".to_string(),
                },
                partstat: match attendee
                    .child("AttendeeStatus")
                    .map(|node| node.text_value().trim())
                {
                    Some("2") => "tentative".to_string(),
                    Some("3") => "accepted".to_string(),
                    Some("4") => "declined".to_string(),
                    _ => "needs-action".to_string(),
                },
                rsvp: false,
            })
        })
        .collect::<Vec<_>>();
    if attendees.is_empty() {
        return None;
    }
    Some(CalendarParticipantsMetadata {
        organizer: None,
        attendees,
    })
}

fn recurrence_to_rrule(recurrence: &WbxmlNode) -> Result<String> {
    let recurrence_type = field_text(recurrence, "Type").unwrap_or_else(|| "0".to_string());
    let mut parts = Vec::new();
    match recurrence_type.as_str() {
        "0" => {
            if let Some(days) =
                field_text(recurrence, "DayOfWeek").and_then(|value| day_of_week_to_rrule(&value))
            {
                parts.push("FREQ=WEEKLY".to_string());
                parts.push(format!("BYDAY={days}"));
            } else {
                parts.push("FREQ=DAILY".to_string());
            }
        }
        "1" => {
            parts.push("FREQ=WEEKLY".to_string());
            if let Some(days) =
                field_text(recurrence, "DayOfWeek").and_then(|value| day_of_week_to_rrule(&value))
            {
                parts.push(format!("BYDAY={days}"));
            }
        }
        "2" => {
            parts.push("FREQ=MONTHLY".to_string());
            let day = field_text(recurrence, "DayOfMonth")
                .ok_or_else(|| anyhow!("monthly recurrence is missing DayOfMonth"))?;
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
        "5" => {
            parts.push("FREQ=YEARLY".to_string());
            let day = field_text(recurrence, "DayOfMonth")
                .ok_or_else(|| anyhow!("yearly recurrence is missing DayOfMonth"))?;
            let month = field_text(recurrence, "MonthOfYear")
                .ok_or_else(|| anyhow!("yearly recurrence is missing MonthOfYear"))?;
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
            parts.push(format!(
                "BYMONTH={}",
                parse_positive_number(&month, "MonthOfYear")?
            ));
        }
        other => bail!("unsupported ActiveSync recurrence type {other}"),
    }
    if let Some(interval) = field_text(recurrence, "Interval")
        .map(|value| parse_positive_number(&value, "Interval"))
        .transpose()?
        .filter(|value| *value > 1)
    {
        parts.push(format!("INTERVAL={interval}"));
    }
    if let Some(count) = field_text(recurrence, "Occurrences")
        .map(|value| parse_positive_number(&value, "Occurrences"))
        .transpose()?
    {
        parts.push(format!("COUNT={count}"));
    }
    if let Some(until) = field_text(recurrence, "Until") {
        parts.push(format!("UNTIL={}", compact_datetime_date(&until)?));
    }
    Ok(parts.join(";"))
}

fn day_of_week_to_rrule(value: &str) -> Option<String> {
    let mask = value.trim().parse::<u32>().ok()?;
    let mut days = Vec::new();
    for (bit, day) in [
        (1, "SU"),
        (2, "MO"),
        (4, "TU"),
        (8, "WE"),
        (16, "TH"),
        (32, "FR"),
        (64, "SA"),
    ] {
        if mask & bit != 0 {
            days.push(day);
        }
    }
    (!days.is_empty()).then(|| days.join(","))
}

fn parse_positive_number(value: &str, field: &str) -> Result<u32> {
    let number = value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{field} must be a positive integer"))?;
    if number == 0 {
        bail!("{field} must be a positive integer");
    }
    Ok(number)
}

fn compact_datetime_date(value: &str) -> Result<String> {
    let compact = value.trim().trim_end_matches('Z');
    let date = compact.split('T').next().unwrap_or_default();
    if date.len() != 8 {
        bail!("invalid ActiveSync recurrence Until");
    }
    Ok(date.to_string())
}

#[cfg(test)]
mod tests {
    use super::parse_contact_input;
    use crate::wbxml::WbxmlNode;
    use lpe_storage::{ClientContact, ContactSourceFields};
    use serde_json::json;

    #[test]
    fn activesync_contact_narrow_update_omits_unowned_rich_fields() {
        let existing = ClientContact {
            id: uuid::Uuid::from_u128(1),
            name: "Ada Example".to_string(),
            email: "ada@example.test".to_string(),
            phone: "+1 555 0100".to_string(),
            addresses_json: json!([{"full": "1 Example Way"}]),
            urls_json: json!([{"url": "https://example.test"}]),
            raw_vcard: Some("BEGIN:VCARD\nEND:VCARD".to_string()),
            source: ContactSourceFields {
                import_source: "carddav".to_string(),
                source_uid: Some("uid-1".to_string()),
                source_etag: Some("etag-1".to_string()),
                source_payload_json: json!({"href": "/contacts/1.vcf"}),
            },
            ..ClientContact::default()
        };
        let mut application_data = WbxmlNode::new(1, "ApplicationData");
        application_data.push(WbxmlNode::with_text(1, "FileAs", "Ada Updated"));

        let input = parse_contact_input(
            uuid::Uuid::from_u128(2),
            Some(existing.id),
            Some(&existing),
            &application_data,
        )
        .unwrap();

        assert_eq!(input.name, "Ada Updated");
        assert_eq!(input.addresses_json, None);
        assert_eq!(input.urls_json, None);
        assert_eq!(input.raw_vcard, None);
        assert!(!input.raw_vcard_is_explicit);
        assert!(!input.source_is_explicit);
    }
}
