use lpe_storage::{
    parse_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    CalendarOrganizerMetadata, CalendarParticipantMetadata, DavTask,
};

pub(crate) fn serialize_vcard(contact: &AccessibleContact) -> String {
    let mut lines = vec![
        "BEGIN:VCARD".to_string(),
        "VERSION:3.0".to_string(),
        format!("UID:{}", contact.id),
        format!("FN:{}", text_escape(&contact.name)),
    ];
    push_line(&mut lines, "TITLE", &contact.role);
    push_line(&mut lines, "EMAIL;TYPE=INTERNET", &contact.email);
    push_line(&mut lines, "TEL", &contact.phone);
    push_line(&mut lines, "ORG", &contact.team);
    push_line(&mut lines, "NOTE", &contact.notes);
    lines.push("END:VCARD".to_string());
    lines.join("\r\n")
}

pub(crate) fn serialize_ical(event: &AccessibleEvent) -> String {
    let dtstart = format_ical_datetime(&event.date, &event.time);
    let metadata = parse_calendar_participants_metadata(&event.attendees_json);
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//DAV Adapter//EN".to_string(),
        "CALSCALE:GREGORIAN".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", event.id),
        format!(
            "{}:{dtstart}",
            property_name_with_tz("DTSTART", &event.time_zone)
        ),
        format!("DURATION:{}", format_duration(event.duration_minutes)),
        format!("SUMMARY:{}", text_escape(&event.title)),
    ];
    push_line(&mut lines, "LOCATION", &event.location);
    push_line(&mut lines, "DESCRIPTION", &event.notes);
    push_line(&mut lines, "RRULE", &event.recurrence_rule);
    if let Some(organizer) = metadata.organizer.as_ref() {
        lines.push(serialize_organizer(organizer));
    }
    for attendee in &metadata.attendees {
        lines.push(serialize_attendee(attendee));
    }
    if metadata.attendees.is_empty() {
        push_line(&mut lines, "X-LPE-ATTENDEES", &event.attendees);
    }
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    lines.join("\r\n")
}

pub(crate) fn serialize_vtodo(task: &DavTask) -> String {
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//DAV Adapter//EN".to_string(),
        "CALSCALE:GREGORIAN".to_string(),
        "BEGIN:VTODO".to_string(),
        format!("UID:{}", task.id),
        format!("SUMMARY:{}", text_escape(&task.title)),
    ];
    push_line(&mut lines, "DESCRIPTION", &task.description);
    push_line(
        &mut lines,
        "STATUS",
        vtodo_status_from_task_status(&task.status),
    );
    if let Some(due_at) = task.due_at.as_deref() {
        lines.push(format!("DUE:{}", format_ical_timestamp(due_at)));
    }
    if let Some(completed_at) = task.completed_at.as_deref() {
        lines.push(format!("COMPLETED:{}", format_ical_timestamp(completed_at)));
    }
    lines.push(format!("X-LPE-SORT-ORDER:{}", task.sort_order));
    lines.push("END:VTODO".to_string());
    lines.push("END:VCALENDAR".to_string());
    lines.join("\r\n")
}

fn push_line(lines: &mut Vec<String>, name: &str, value: &str) {
    if !value.trim().is_empty() {
        lines.push(format!("{name}:{}", text_escape(value)));
    }
}

pub(crate) fn format_ical_datetime(date: &str, time: &str) -> String {
    format!("{}T{}00", date.replace('-', ""), time.replace(':', ""))
}

pub(crate) fn format_ical_timestamp(value: &str) -> String {
    value
        .replace('-', "")
        .replace(':', "")
        .trim_end_matches(".000")
        .to_string()
}

pub(crate) fn format_duration(minutes: i32) -> String {
    if minutes <= 0 {
        return "PT0S".to_string();
    }
    if minutes % 60 == 0 {
        return format!("PT{}H", minutes / 60);
    }
    format!("PT{}M", minutes)
}

pub(crate) fn property_name_with_tz(name: &str, time_zone: &str) -> String {
    let time_zone = time_zone.trim();
    if time_zone.is_empty() {
        return name.to_string();
    }
    format!("{name};TZID={time_zone}")
}

fn serialize_organizer(organizer: &CalendarOrganizerMetadata) -> String {
    let mut property = "ORGANIZER".to_string();
    if !organizer.common_name.trim().is_empty() {
        property.push_str(&format!(";CN={}", param_escape(&organizer.common_name)));
    }
    let value = if organizer.email.trim().is_empty() {
        "mailto:unknown@example.invalid".to_string()
    } else if organizer.email.to_ascii_lowercase().starts_with("mailto:") {
        organizer.email.clone()
    } else {
        format!("mailto:{}", organizer.email.trim())
    };
    format!("{property}:{value}")
}

fn serialize_attendee(attendee: &CalendarParticipantMetadata) -> String {
    let mut property = "ATTENDEE".to_string();
    if !attendee.common_name.trim().is_empty() {
        property.push_str(&format!(";CN={}", param_escape(&attendee.common_name)));
    }
    if !attendee.role.trim().is_empty() {
        property.push_str(&format!(";ROLE={}", attendee.role.trim()));
    }
    if !attendee.partstat.trim().is_empty() {
        property.push_str(&format!(
            ";PARTSTAT={}",
            attendee.partstat.trim().to_ascii_uppercase()
        ));
    }
    if attendee.rsvp {
        property.push_str(";RSVP=TRUE");
    }
    let value = if attendee.email.trim().is_empty() {
        "mailto:unknown@example.invalid".to_string()
    } else if attendee.email.to_ascii_lowercase().starts_with("mailto:") {
        attendee.email.clone()
    } else {
        format!("mailto:{}", attendee.email.trim())
    };
    format!("{property}:{value}")
}

fn vtodo_status_from_task_status(status: &str) -> &'static str {
    match status {
        "needs-action" => "NEEDS-ACTION",
        "in-progress" => "IN-PROCESS",
        "completed" => "COMPLETED",
        "cancelled" => "CANCELLED",
        _ => "NEEDS-ACTION",
    }
}

fn param_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "'")
        .replace(';', "\\;")
}

fn text_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(';', "\\;")
        .replace(',', "\\,")
}

pub(crate) fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
