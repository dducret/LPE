use super::super::*;
use crate::ews_types::{EwsMonth, EwsResponseType, EwsWeekday};

pub(in crate::service) fn calendar_change_key(event: &AccessibleEvent, version: &str) -> String {
    versioned_change_key("calendar", &event.id.to_string(), version)
}

pub(in crate::service) fn calendar_item_summary_xml_with_change_key(
    event: &AccessibleEvent,
    change_key: &str,
) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

pub(in crate::service) fn calendar_item_xml_with_change_key(
    event: &AccessibleEvent,
    change_key: &str,
) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Location>{location}</t:Location>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "{recurrence}",
            "<t:LegacyFreeBusyStatus>Busy</t:LegacyFreeBusyStatus>",
            "{attendees}",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        location = escape_xml(&event.location),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
        recurrence = ews_recurrence_xml(event),
        attendees = ews_attendees_xml(event),
        notes = escape_xml(&event.notes),
    )
}

pub(in crate::service) fn create_event_success_response(
    event: &AccessibleEvent,
    change_key: &str,
) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

pub(in crate::service) fn ews_datetime(date: &str, time: &str) -> String {
    format!("{}T{}:00Z", date.trim(), time.trim())
}

pub(in crate::service) fn event_end_datetime(event: &AccessibleEvent) -> String {
    let start_minutes = time_minutes(&event.time).unwrap_or(0);
    let total_minutes = i64::from(start_minutes) + i64::from(event.duration_minutes.max(0));
    let end_date = ews_date_after_days(&event.date, total_minutes / 1_440)
        .unwrap_or_else(|| event.date.clone());
    let end_minutes = total_minutes % 1_440;
    ews_datetime(
        &end_date,
        &format!("{:02}:{:02}", end_minutes / 60, end_minutes % 60),
    )
}

pub(in crate::service) fn parse_create_event_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientEventInput> {
    // [MS-OXWSMTGS] §3.1.4.2: validate the bounded CalendarItem time range
    // before the canonical calendar create path is called.
    let event = element_content(request, "CalendarItem")
        .ok_or_else(|| anyhow!("CreateItem is missing CalendarItem"))?;
    let start = element_text(event, "Start").unwrap_or_default();
    let end = element_text(event, "End").unwrap_or_default();
    let (date, time) = ews_datetime_parts(&start)
        .ok_or_else(|| anyhow!("CalendarItem is missing a valid Start value"))?;
    let duration_minutes = ews_duration_minutes(&start, &end)
        .filter(|duration| *duration > 0)
        .ok_or_else(|| anyhow!("CalendarItem End must be later than Start"))?;
    let body_tag = open_tag_text(event, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(event, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value.clone()
    };
    let (participants, _) = parse_event_participants(principal, event);

    Ok(UpsertClientEventInput {
        id: None,
        account_id: principal.account_id,
        uid: String::new(),
        date,
        time,
        time_zone: requested_time_zone(request)?.unwrap_or_else(|| "UTC".to_string()),
        duration_minutes,
        all_day: element_text(event, "IsAllDayEvent")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: parse_ews_recurrence(event)?,
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: element_text(event, "Subject").unwrap_or_else(|| "Untitled event".to_string()),
        location: element_text(event, "Location").unwrap_or_default(),
        organizer_json: participants
            .organizer
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(|| "{}".to_string()),
        attendees: calendar_attendee_labels(&participants),
        attendees_json: serialize_calendar_participants_metadata(&participants),
        notes,
        body_html: if body_type.eq_ignore_ascii_case("HTML") {
            body_value
        } else {
            String::new()
        },
    })
}

pub(in crate::service) fn parse_update_event_input(
    principal: &AccountPrincipal,
    existing: &AccessibleEvent,
    request: &str,
) -> Result<UpsertClientEventInput> {
    // [MS-OXWSMTGS] §3.1.4.8: validate each supplied endpoint before the
    // canonical update path; a Start-only update keeps its existing duration.
    let event = element_content(request, "CalendarItem").unwrap_or(request);
    let start = element_text(event, "Start");
    let end = element_text(event, "End");
    let (date, time) = match start.as_deref() {
        Some(value) => {
            ews_datetime_parts(value).ok_or_else(|| anyhow!("CalendarItem Start is invalid"))?
        }
        None => (existing.date.clone(), existing.time.clone()),
    };
    let duration_minutes = match (start.as_deref(), end.as_deref()) {
        (Some(start), Some(end)) => ews_duration_minutes(start, end)
            .filter(|duration| *duration > 0)
            .ok_or_else(|| anyhow!("CalendarItem End must be later than Start"))?,
        (Some(_), None) => existing.duration_minutes,
        (None, Some(end)) => {
            ews_duration_minutes(&format!("{}T{}:00Z", existing.date, existing.time), end)
                .filter(|duration| *duration > 0)
                .ok_or_else(|| anyhow!("CalendarItem End must be later than Start"))?
        }
        _ => existing.duration_minutes,
    };
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(event, "Body") {
        let body_tag = open_tag_text(event, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };
    let (participants, has_attendee_updates) = parse_event_participants(principal, event);

    Ok(UpsertClientEventInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        uid: existing.uid.clone(),
        date,
        time,
        time_zone: requested_time_zone(request)?.unwrap_or_else(|| existing.time_zone.clone()),
        duration_minutes,
        all_day: element_text(event, "IsAllDayEvent")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(existing.all_day),
        status: existing.status.clone(),
        sequence: existing.sequence,
        recurrence_rule: if field_deleted(request, "calendar:Recurrence") {
            String::new()
        } else if element_content(event, "Recurrence").is_some() {
            parse_ews_recurrence(event)?
        } else {
            existing.recurrence_rule.clone()
        },
        recurrence_json: existing.recurrence_json.clone(),
        recurrence_exceptions_json: existing.recurrence_exceptions_json.clone(),
        title: deleted_or_updated_text(
            request,
            event,
            "calendar:Subject",
            "Subject",
            &existing.title,
        )
        .if_empty(existing.title.clone()),
        location: deleted_or_updated_text(
            request,
            event,
            "calendar:Location",
            "Location",
            &existing.location,
        ),
        organizer_json: existing.organizer_json.clone(),
        attendees: if has_attendee_updates {
            calendar_attendee_labels(&participants)
        } else {
            existing.attendees.clone()
        },
        attendees_json: if has_attendee_updates {
            serialize_calendar_participants_metadata(&participants)
        } else {
            existing.attendees_json.clone()
        },
        notes,
        body_html: if let Some(body_value) = element_text(event, "Body") {
            let body_tag = open_tag_text(event, "Body").unwrap_or_default();
            let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
            if body_type.eq_ignore_ascii_case("HTML") {
                body_value
            } else {
                existing.body_html.clone()
            }
        } else {
            existing.body_html.clone()
        },
    })
}

fn ews_attendees_xml(event: &AccessibleEvent) -> String {
    let metadata = parse_calendar_participants_metadata(&event.attendees_json);
    let required = ews_attendee_collection_xml(
        "RequiredAttendees",
        metadata
            .attendees
            .iter()
            .filter(|attendee| !attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    );
    let optional = ews_attendee_collection_xml(
        "OptionalAttendees",
        metadata
            .attendees
            .iter()
            .filter(|attendee| attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    );
    format!("{required}{optional}")
}

fn ews_attendee_collection_xml<'a>(
    element_name: &str,
    attendees: impl Iterator<Item = &'a CalendarParticipantMetadata>,
) -> String {
    let attendees = attendees.map(ews_attendee_xml).collect::<String>();
    if attendees.is_empty() {
        String::new()
    } else {
        format!("<t:{element_name}>{attendees}</t:{element_name}>")
    }
}

fn ews_attendee_xml(attendee: &CalendarParticipantMetadata) -> String {
    format!(
        concat!(
            "<t:Attendee>",
            "<t:Mailbox>",
            "<t:Name>{}</t:Name>",
            "<t:EmailAddress>{}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "</t:Mailbox>",
            "<t:ResponseType>{}</t:ResponseType>",
            "</t:Attendee>"
        ),
        escape_xml(&attendee.common_name),
        escape_xml(&attendee.email),
        partstat_to_ews_response_type(&attendee.partstat),
    )
}

fn partstat_to_ews_response_type(partstat: &str) -> &'static str {
    match partstat.trim().to_ascii_lowercase().as_str() {
        "accepted" => "Accept",
        "tentative" => "Tentative",
        "declined" => "Decline",
        _ => "NoResponseReceived",
    }
}

fn ews_recurrence_xml(event: &AccessibleEvent) -> String {
    let Some(recurrence) = rrule_to_ews_recurrence(&event.recurrence_rule, &event.date) else {
        return String::new();
    };
    recurrence
}

fn rrule_to_ews_recurrence(rrule: &str, start_date: &str) -> Option<String> {
    let fields = rrule_fields(rrule);
    let freq = fields.get("FREQ")?.as_str();
    let interval = fields
        .get("INTERVAL")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    let pattern = match freq {
        "DAILY" => format!(
            "<t:DailyRecurrence><t:Interval>{}</t:Interval></t:DailyRecurrence>",
            escape_xml(&interval)
        ),
        "WEEKLY" => {
            let days = fields
                .get("BYDAY")
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(rrule_weekday_to_ews)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "Monday".to_string());
            format!(
                concat!(
                    "<t:WeeklyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DaysOfWeek>{days}</t:DaysOfWeek>",
                    "</t:WeeklyRecurrence>"
                ),
                interval = escape_xml(&interval),
                days = escape_xml(&days),
            )
        }
        "MONTHLY" => {
            let day = fields.get("BYMONTHDAY")?;
            format!(
                concat!(
                    "<t:AbsoluteMonthlyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "</t:AbsoluteMonthlyRecurrence>"
                ),
                interval = escape_xml(&interval),
                day = escape_xml(day),
            )
        }
        "YEARLY" => {
            let day = fields.get("BYMONTHDAY")?;
            let month = fields.get("BYMONTH").and_then(|value| {
                value
                    .parse::<u32>()
                    .ok()
                    .and_then(rrule_month_number_to_ews)
            })?;
            format!(
                concat!(
                    "<t:AbsoluteYearlyRecurrence>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "<t:Month>{month}</t:Month>",
                    "</t:AbsoluteYearlyRecurrence>"
                ),
                day = escape_xml(day),
                month = month,
            )
        }
        _ => return None,
    };
    let range = if let Some(count) = fields.get("COUNT") {
        format!(
            concat!(
                "<t:NumberedRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:NumberOfOccurrences>{}</t:NumberOfOccurrences>",
                "</t:NumberedRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(count),
        )
    } else if let Some(until) = fields.get("UNTIL") {
        format!(
            concat!(
                "<t:EndDateRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:EndDate>{}</t:EndDate>",
                "</t:EndDateRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(&rrule_until_to_ews_date(until)),
        )
    } else {
        format!(
            "<t:NoEndRecurrence><t:StartDate>{}</t:StartDate></t:NoEndRecurrence>",
            escape_xml(start_date)
        )
    };
    Some(format!("<t:Recurrence>{pattern}{range}</t:Recurrence>"))
}

fn rrule_fields(rrule: &str) -> HashMap<String, String> {
    rrule
        .split(';')
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| (key.trim().to_ascii_uppercase(), value.trim().to_string()))
        .collect()
}

fn rrule_weekday_to_ews(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_uppercase().as_str() {
        "MO" => Some("Monday"),
        "TU" => Some("Tuesday"),
        "WE" => Some("Wednesday"),
        "TH" => Some("Thursday"),
        "FR" => Some("Friday"),
        "SA" => Some("Saturday"),
        "SU" => Some("Sunday"),
        _ => None,
    }
}

fn rrule_month_number_to_ews(value: u32) -> Option<&'static str> {
    match value {
        1 => Some("January"),
        2 => Some("February"),
        3 => Some("March"),
        4 => Some("April"),
        5 => Some("May"),
        6 => Some("June"),
        7 => Some("July"),
        8 => Some("August"),
        9 => Some("September"),
        10 => Some("October"),
        11 => Some("November"),
        12 => Some("December"),
        _ => None,
    }
}

fn rrule_until_to_ews_date(value: &str) -> String {
    let date = value.split('T').next().unwrap_or(value);
    if date.len() == 8 {
        format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
    } else {
        date.to_string()
    }
}

pub(in crate::service) fn parse_ews_recurrence(event: &str) -> Result<String> {
    let Some(recurrence) = element_content(event, "Recurrence") else {
        return Ok(String::new());
    };

    let mut parts = Vec::new();
    if let Some(daily) = element_content(recurrence, "DailyRecurrence") {
        parts.push("FREQ=DAILY".to_string());
        push_interval_part(&mut parts, daily);
    } else if let Some(weekly) = element_content(recurrence, "WeeklyRecurrence") {
        parts.push("FREQ=WEEKLY".to_string());
        push_interval_part(&mut parts, weekly);
        if let Some(days) = element_text(weekly, "DaysOfWeek") {
            let byday = days
                .split_whitespace()
                .map(ews_weekday_to_rrule)
                .collect::<Result<Vec<_>>>()?;
            if !byday.is_empty() {
                parts.push(format!("BYDAY={}", byday.join(",")));
            }
        }
    } else if let Some(monthly) = element_content(recurrence, "AbsoluteMonthlyRecurrence") {
        parts.push("FREQ=MONTHLY".to_string());
        push_interval_part(&mut parts, monthly);
        if let Some(day) = element_text(monthly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
    } else if let Some(yearly) = element_content(recurrence, "AbsoluteYearlyRecurrence") {
        parts.push("FREQ=YEARLY".to_string());
        if let Some(day) = element_text(yearly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
        if let Some(month) = element_text(yearly, "Month") {
            parts.push(format!("BYMONTH={}", ews_month_to_number(&month)?));
        }
    } else {
        bail!("unsupported EWS recurrence pattern");
    }

    if let Some(numbered) = element_content(recurrence, "NumberedRecurrence") {
        if let Some(count) = element_text(numbered, "NumberOfOccurrences") {
            parts.push(format!(
                "COUNT={}",
                parse_positive_number(&count, "NumberOfOccurrences")?
            ));
        }
    } else if let Some(end_date) = element_content(recurrence, "EndDateRecurrence") {
        if let Some(end) = element_text(end_date, "EndDate") {
            parts.push(format!("UNTIL={}", rrule_date(&end)?));
        }
    }

    Ok(parts.join(";"))
}

fn push_interval_part(parts: &mut Vec<String>, recurrence: &str) {
    if let Some(interval) = element_text(recurrence, "Interval")
        .and_then(|value| parse_positive_number(&value, "Interval").ok())
        .filter(|value| *value > 1)
    {
        parts.push(format!("INTERVAL={interval}"));
    }
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

fn ews_weekday_to_rrule(value: &str) -> Result<&'static str> {
    Ok(EwsWeekday::parse(value)?.rrule_day())
}

fn ews_month_to_number(value: &str) -> Result<u32> {
    Ok(EwsMonth::parse(value)?.number())
}

fn rrule_date(value: &str) -> Result<String> {
    let date = value.trim().split('T').next().unwrap_or_default();
    let mut parts = date.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("recurrence end date must be YYYY-MM-DD");
    };
    Ok(format!("{year}{month}{day}"))
}

fn parse_event_participants(
    principal: &AccountPrincipal,
    event: &str,
) -> (CalendarParticipantsMetadata, bool) {
    let mut metadata = CalendarParticipantsMetadata {
        organizer: Some(CalendarOrganizerMetadata {
            email: principal.email.clone(),
            common_name: principal.display_name.clone(),
        }),
        attendees: Vec::new(),
    };
    let mut has_attendee_collections = false;
    for (collection_name, role) in [
        ("RequiredAttendees", "REQ-PARTICIPANT"),
        ("OptionalAttendees", "OPT-PARTICIPANT"),
    ] {
        let Some(collection) = element_content(event, collection_name) else {
            continue;
        };
        has_attendee_collections = true;
        metadata.attendees.extend(
            element_contents(collection, "Attendee")
                .into_iter()
                .filter_map(|attendee| parse_attendee(attendee, role)),
        );
    }
    (metadata, has_attendee_collections)
}

fn parse_attendee(attendee: &str, role: &str) -> Option<CalendarParticipantMetadata> {
    let mailbox = element_content(attendee, "Mailbox").and_then(parse_mailbox)?;
    Some(CalendarParticipantMetadata {
        email: mailbox.address,
        common_name: mailbox.display_name.unwrap_or_default(),
        role: role.to_string(),
        partstat: ews_response_type_to_partstat(&element_text(attendee, "ResponseType")),
        rsvp: false,
    })
}

fn ews_response_type_to_partstat(response_type: &Option<String>) -> String {
    EwsResponseType::parse(response_type.as_deref().unwrap_or_default())
        .partstat()
        .to_string()
}

fn requested_time_zone(request: &str) -> Result<Option<String>> {
    // [MS-OXWSGTZ] §3.1.4.1: accept only identifiers in the published EWS catalog.
    let Some(time_zone) = open_tag_text(request, "TimeZoneDefinition") else {
        return Ok(None);
    };
    let id = attribute_value(time_zone, "Id")
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow!("TimeZoneDefinition requires a non-empty supported Id"))?;
    canonical_ews_time_zone(id)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("unsupported TimeZoneDefinition Id"))
        .map(Some)
}

fn ews_datetime_parts(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    if !matches!(trimmed.len(), 16 | 17 | 19 | 20)
        || trimmed.get(10..11) != Some("T")
        || !matches!(trimmed.get(16..), Some("" | "Z" | ":00" | ":00Z"))
    {
        return None;
    }
    let date = trimmed.get(0..10)?;
    let time = trimmed.get(11..16)?;
    let date_bytes = date.as_bytes();
    let time_bytes = time.as_bytes();
    let date_is_yyyy_mm_dd = date_bytes[4] == b'-'
        && date_bytes[7] == b'-'
        && date_bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit());
    let time_is_hh_mm = time_bytes[2] == b':'
        && time_bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 2 || byte.is_ascii_digit());
    (date_is_yyyy_mm_dd
        && time_is_hh_mm
        && ews_date_parts(date).is_some()
        && time_minutes(time).is_some())
    .then(|| (date.to_string(), time.to_string()))
}

fn ews_duration_minutes(start: &str, end: &str) -> Option<i32> {
    let start_minutes = ews_datetime_minutes(start)?;
    let end_minutes = ews_datetime_minutes(end)?;
    i32::try_from(end_minutes - start_minutes)
        .ok()
        .filter(|duration| *duration >= 0)
}

fn time_minutes(value: &str) -> Option<i32> {
    let (hour, minute) = value.split_once(':')?;
    let hour = hour.parse::<i32>().ok()?;
    let minute = minute.parse::<i32>().ok()?;
    ((0..24).contains(&hour) && (0..60).contains(&minute)).then_some(hour * 60 + minute)
}

fn ews_datetime_minutes(value: &str) -> Option<i64> {
    let (date, time) = ews_datetime_parts(value)?;
    Some(days_from_civil(ews_date_parts(&date)?)? * 1_440 + i64::from(time_minutes(&time)?))
}

fn ews_date_after_days(date: &str, days: i64) -> Option<String> {
    let date = days_from_civil(ews_date_parts(date)?)?.checked_add(days)?;
    let (year, month, day) = civil_from_days(date);
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn ews_date_parts(value: &str) -> Option<(i32, u32, u32)> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;
    (parts.next().is_none()
        && (1..=12).contains(&month)
        && (1..=days_in_month(year, month)).contains(&day))
    .then_some((year, month, day))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil((year, month, day): (i32, u32, u32)) -> Option<i64> {
    let year = i64::from(year) - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    day_of_era
        .checked_add(era.checked_mul(146_097)?)?
        .checked_sub(719_468)
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_parameter = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_parameter + 2) / 5 + 1;
    let month = month_parameter + if month_parameter < 10 { 3 } else { -9 };
    (year + if month <= 2 { 1 } else { 0 }, month, day)
}
