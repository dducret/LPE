use super::super::*;

pub(in crate::service) fn calendar_change_key(
    event: &AccessibleEvent,
    sync_version: Option<&str>,
) -> String {
    stable_change_key(&[
        "calendar",
        &event.id.to_string(),
        sync_version.unwrap_or_default(),
        &event.collection_id,
        &event.date,
        &event.time,
        &event.time_zone,
        &event.duration_minutes.to_string(),
        &event.recurrence_rule,
        &event.title,
        &event.location,
        &event.attendees,
        &event.attendees_json,
        &event.notes,
    ])
}

pub(in crate::service) fn calendar_item_summary_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_summary_xml_with_change_key(event, &change_key)
}

fn calendar_item_summary_xml_with_change_key(event: &AccessibleEvent, change_key: &str) -> String {
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

pub(in crate::service) fn calendar_item_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_xml_with_change_key(event, &change_key)
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

pub(in crate::service) fn create_event_success_response(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"created\"/>",
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
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
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
