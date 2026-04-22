use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, normalize_calendar_participation_status,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput,
};
use uuid::Uuid;

pub(crate) fn parse_vcard(
    id: Uuid,
    account_id: Uuid,
    body: &[u8],
) -> Result<UpsertClientContactInput> {
    let content = std::str::from_utf8(body)?;
    let mut name = String::new();
    let mut role = String::new();
    let mut email = String::new();
    let mut phone = String::new();
    let mut team = String::new();
    let mut notes = String::new();

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "FN" => name = value,
            "TITLE" => role = value,
            "EMAIL" => email = value,
            "TEL" => phone = value,
            "ORG" => team = value,
            "NOTE" => notes = value,
            _ => {}
        }
    }

    if name.trim().is_empty() || email.trim().is_empty() {
        bail!("contact name and email are required");
    }

    Ok(UpsertClientContactInput {
        id: Some(id),
        account_id,
        name,
        role,
        email,
        phone,
        team,
        notes,
    })
}

pub(crate) fn parse_ical(
    id: Uuid,
    account_id: Uuid,
    body: &[u8],
) -> Result<UpsertClientEventInput> {
    let content = std::str::from_utf8(body)?;
    let mut date = String::new();
    let mut time = String::new();
    let mut time_zone = String::new();
    let mut duration_minutes = 0;
    let mut recurrence_rule = String::new();
    let mut title = String::new();
    let mut location = String::new();
    let mut attendees = String::new();
    let mut metadata = CalendarParticipantsMetadata::default();
    let mut notes = String::new();

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "DTSTART" => {
                let (parsed_date, parsed_time) = parse_ical_datetime(&value)?;
                date = parsed_date;
                time = parsed_time;
                time_zone = property_parameter(left, "TZID").unwrap_or_default();
            }
            "DURATION" => duration_minutes = parse_ical_duration(&value)?,
            "RRULE" => recurrence_rule = value,
            "SUMMARY" => title = value,
            "LOCATION" => location = value,
            "DESCRIPTION" => notes = value,
            "X-LPE-ATTENDEES" => attendees = value,
            "ORGANIZER" => metadata.organizer = parse_organizer(left, &value)?,
            "ATTENDEE" => metadata.attendees.push(parse_attendee(left, &value)?),
            _ => {}
        }
    }

    if date.is_empty() || time.is_empty() || title.trim().is_empty() {
        bail!("event date, time, and title are required");
    }

    if !metadata.attendees.is_empty() {
        attendees = calendar_attendee_labels(&metadata);
    }

    Ok(UpsertClientEventInput {
        id: Some(id),
        account_id,
        date,
        time,
        time_zone,
        duration_minutes,
        recurrence_rule,
        title,
        location,
        attendees,
        attendees_json: serialize_calendar_participants_metadata(&metadata),
        notes,
    })
}

pub(crate) fn parse_vtodo(
    id: Uuid,
    account_id: Uuid,
    collection_id: Option<&str>,
    body: &[u8],
) -> Result<UpsertClientTaskInput> {
    let content = std::str::from_utf8(body)?;
    let mut title = String::new();
    let mut description = String::new();
    let mut status = String::new();
    let mut due_at = None;
    let mut completed_at = None;
    let mut sort_order = 0;

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "SUMMARY" => title = value,
            "DESCRIPTION" => description = value,
            "STATUS" => status = task_status_from_vtodo_status(&value)?,
            "DUE" => due_at = Some(parse_ical_timestamp(&value)?),
            "COMPLETED" => completed_at = Some(parse_ical_timestamp(&value)?),
            "X-LPE-SORT-ORDER" => {
                sort_order = value
                    .parse::<i32>()
                    .map_err(|_| anyhow!("invalid X-LPE-SORT-ORDER"))?;
            }
            _ => {}
        }
    }

    if title.trim().is_empty() {
        bail!("VTODO summary is required");
    }

    Ok(UpsertClientTaskInput {
        id: Some(id),
        principal_account_id: account_id,
        account_id,
        task_list_id: collection_id.map(Uuid::parse_str).transpose()?,
        title,
        description,
        status,
        due_at,
        completed_at,
        sort_order,
    })
}

fn parse_ical_datetime(value: &str) -> Result<(String, String)> {
    let compact = value.trim_end_matches('Z');
    let (date_part, time_part) = compact
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid DTSTART"))?;
    if date_part.len() != 8 || time_part.len() < 4 {
        bail!("invalid DTSTART");
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

fn parse_ical_timestamp(value: &str) -> Result<String> {
    let value = value.trim().trim_end_matches('Z');
    if value.len() == 8 {
        return Ok(format!(
            "{}-{}-{}T00:00:00Z",
            &value[0..4],
            &value[4..6],
            &value[6..8]
        ));
    }
    let (date, time) = parse_ical_datetime(value)?;
    Ok(format!("{date}T{}:00Z", time))
}

fn parse_ical_duration(value: &str) -> Result<i32> {
    let value = value.trim();
    if value == "PT0S" {
        return Ok(0);
    }
    let Some(value) = value.strip_prefix("PT") else {
        bail!("invalid DURATION");
    };
    if let Some(hours) = value.strip_suffix('H') {
        return hours
            .parse::<i32>()
            .map(|value| value.max(0) * 60)
            .map_err(|_| anyhow!("invalid DURATION"));
    }
    if let Some(minutes) = value.strip_suffix('M') {
        return minutes
            .parse::<i32>()
            .map(|value| value.max(0))
            .map_err(|_| anyhow!("invalid DURATION"));
    }
    bail!("invalid DURATION")
}

fn property_parameter(left: &str, name: &str) -> Option<String> {
    left.split(';').skip(1).find_map(|segment| {
        let (key, value) = segment.split_once('=')?;
        if key.eq_ignore_ascii_case(name) {
            Some(text_unescape(value.trim_matches('"')))
        } else {
            None
        }
    })
}

fn parse_organizer(left: &str, value: &str) -> Result<Option<CalendarOrganizerMetadata>> {
    let email = normalize_calendar_email(value);
    let common_name = property_parameter(left, "CN").unwrap_or_default();
    if email.is_empty() && common_name.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(CalendarOrganizerMetadata { email, common_name }))
}

fn parse_attendee(left: &str, value: &str) -> Result<CalendarParticipantMetadata> {
    let email = normalize_calendar_email(value);
    if email.is_empty() {
        bail!("ATTENDEE email is required");
    }
    Ok(CalendarParticipantMetadata {
        email,
        common_name: property_parameter(left, "CN").unwrap_or_default(),
        role: property_parameter(left, "ROLE").unwrap_or_else(|| "REQ-PARTICIPANT".to_string()),
        partstat: normalize_calendar_participation_status(
            &property_parameter(left, "PARTSTAT").unwrap_or_else(|| "NEEDS-ACTION".to_string()),
        ),
        rsvp: property_parameter(left, "RSVP")
            .map(|value| value.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false),
    })
}

fn task_status_from_vtodo_status(status: &str) -> Result<String> {
    let normalized = status.trim();
    if normalized.is_empty() {
        return Ok("needs-action".to_string());
    }
    match normalized.to_ascii_uppercase().as_str() {
        "NEEDS-ACTION" => Ok("needs-action".to_string()),
        "IN-PROCESS" => Ok("in-progress".to_string()),
        "COMPLETED" => Ok("completed".to_string()),
        "CANCELLED" => Ok("cancelled".to_string()),
        _ => bail!("invalid VTODO STATUS"),
    }
}

fn unfolded_lines(content: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for raw in content.lines() {
        let line = raw.trim_end_matches('\r');
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = lines.last_mut() {
                last.push_str(line.trim_start());
            }
        } else {
            lines.push(line.to_string());
        }
    }
    lines
}

fn text_unescape(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
}
