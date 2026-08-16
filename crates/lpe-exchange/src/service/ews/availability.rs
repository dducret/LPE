use super::super::*;
use lpe_domain::{civil_from_days, days_from_civil};

// [MS-OXWSGTZ] §3.1.4.1.3.3 permits compact identifier/name projections.
const EWS_TIME_ZONE_CATALOG: [(&str, &str); 2] = [
    ("UTC", "(UTC) Coordinated Universal Time"),
    (
        "Europe/Berlin",
        "(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna",
    ),
];

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_user_availability(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let (mailbox_email, window_start, window_end) = match parse_availability_request(request) {
            Ok(request) => request,
            Err(message) => return Ok(get_user_availability_error_response(&message)),
        };
        let collections = self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?;
        let collection_ids = collections
            .into_iter()
            .filter(|collection| {
                collection.rights.may_read
                    && collection.owner_email.eq_ignore_ascii_case(&mailbox_email)
            })
            .map(|collection| collection.id)
            .collect::<std::collections::BTreeSet<_>>();
        if collection_ids.is_empty() {
            return Ok(get_user_availability_error_response(
                "No readable canonical calendar is available for the requested mailbox.",
            ));
        }

        let mut events = Vec::new();
        for collection_id in collection_ids {
            events.extend(
                self.store
                    .fetch_accessible_events_in_collection(principal.account_id, &collection_id)
                    .await?,
            );
        }
        let mut events = events
            .into_iter()
            .filter(|event| {
                event.rights.may_read
                    && event.owner_email.eq_ignore_ascii_case(&mailbox_email)
                    && !event.status.eq_ignore_ascii_case("cancelled")
            })
            .flat_map(|event| expand_availability_event(&event, window_start, window_end))
            .filter(|event| event_overlaps_window(event, window_start, window_end))
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            availability_event_start_minutes(left).cmp(&availability_event_start_minutes(right))
        });
        Ok(get_user_availability_success_response(
            &events,
            availability_suggestions_response(request).as_deref(),
        ))
    }
}

pub(in crate::service) fn get_server_time_zones_response(request: &str) -> String {
    let ids = match requested_server_time_zone_ids(request) {
        Ok(ids) => ids,
        Err(message) => {
            return operation_error_response("GetServerTimeZones", "ErrorInvalidRequest", &message);
        }
    };
    let definitions = EWS_TIME_ZONE_CATALOG
        .iter()
        .filter(|(id, _)| {
            ids.as_ref()
                .is_none_or(|ids| ids.iter().any(|requested| requested == id))
        })
        .map(|(id, name)| {
            format!(
                "<t:TimeZoneDefinition Id=\"{}\" Name=\"{}\"/>",
                escape_xml(id),
                escape_xml(name),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetServerTimeZonesResponse>",
            "<m:ResponseMessages>",
            "<m:GetServerTimeZonesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:TimeZoneDefinitions>",
            "{definitions}",
            "</m:TimeZoneDefinitions>",
            "</m:GetServerTimeZonesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetServerTimeZonesResponse>"
        ),
        definitions = definitions,
    )
}

pub(in crate::service) fn canonical_ews_time_zone(value: &str) -> Option<&'static str> {
    EWS_TIME_ZONE_CATALOG
        .iter()
        .find_map(|(id, _)| id.eq_ignore_ascii_case(value.trim()).then_some(*id))
}

pub(in crate::service) fn get_user_availability_success_response(
    events: &[AccessibleEvent],
    suggestions_response: Option<&str>,
) -> String {
    let events = events
        .iter()
        .map(|event| {
            format!(
                concat!(
                    "<t:CalendarEvent>",
                    "<t:StartTime>{}</t:StartTime>",
                    "<t:EndTime>{}</t:EndTime>",
                    "<t:BusyType>Busy</t:BusyType>",
                    "</t:CalendarEvent>"
                ),
                escape_xml(&availability_event_start_datetime(event)),
                escape_xml(&availability_event_end_datetime(event)),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:FreeBusyView>",
            "<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>",
            "<t:CalendarEventArray>{events}</t:CalendarEventArray>",
            "</m:FreeBusyView>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "{suggestions_response}",
            "</m:GetUserAvailabilityResponse>"
        ),
        events = events,
        suggestions_response = suggestions_response.unwrap_or_default(),
    )
}

pub(in crate::service) fn availability_suggestions_response(request: &str) -> Option<String> {
    element_content(request, "SuggestionsViewOptions")?;
    let suggestion_start = element_content(request, "DetailedSuggestionsWindow")
        .and_then(|window| element_text(window, "StartTime"))
        .or_else(|| {
            element_content(request, "TimeWindow")
                .and_then(|window| element_text(window, "StartTime"))
        })
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());
    let suggestion_date = suggestion_start
        .split('T')
        .next()
        .filter(|date| !date.trim().is_empty())
        .unwrap_or("1970-01-01");
    Some(format!(
        concat!(
            "<m:SuggestionsResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:SuggestionDayResultArray>",
            "<t:SuggestionDayResult>",
            "<t:Date>{suggestion_date}T00:00:00Z</t:Date>",
            "<t:DayQuality>Fair</t:DayQuality>",
            "<t:SuggestionArray></t:SuggestionArray>",
            "</t:SuggestionDayResult>",
            "</m:SuggestionDayResultArray>",
            "</m:SuggestionsResponse>"
        ),
        suggestion_date = escape_xml(suggestion_date),
    ))
}

pub(in crate::service) fn event_overlaps_window(
    event: &AccessibleEvent,
    start: i64,
    end: i64,
) -> bool {
    let Some(event_start) = availability_event_start_minutes(event) else {
        return false;
    };
    let event_end = event_start.saturating_add(i64::from(event.duration_minutes.max(0)));
    event_end > start && event_start < end
}

fn requested_server_time_zone_ids(
    request: &str,
) -> std::result::Result<Option<Vec<String>>, String> {
    let Some(open_tag) = open_tag_text(request, "GetServerTimeZones") else {
        return Err("GetServerTimeZones is required.".to_string());
    };
    match attribute_value(open_tag, "ReturnFullTimeZoneData") {
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => {}
        Some(_) => return Err("LPE supports only compact time-zone definitions.".to_string()),
        None => return Err("ReturnFullTimeZoneData=\"false\" is required.".to_string()),
    }
    let id_sets = element_contents(request, "Ids");
    if id_sets.len() > 1 {
        return Err("GetServerTimeZones accepts at most one Ids element.".to_string());
    }
    let Some(id_set) = id_sets.into_iter().next() else {
        return Ok(None);
    };
    let mut ids = element_contents(id_set, "Id")
        .into_iter()
        .map(xml_text)
        .map(|id| id.trim().to_string())
        .collect::<Vec<_>>();
    if ids.is_empty() || ids.iter().any(String::is_empty) {
        return Err("Ids must contain one or more non-empty supported identifiers.".to_string());
    }
    ids.sort_by_key(|id| id.to_ascii_lowercase());
    if ids
        .windows(2)
        .any(|ids| ids[0].eq_ignore_ascii_case(&ids[1]))
        || ids.iter().any(|id| canonical_ews_time_zone(id).is_none())
    {
        return Err("Ids contains an unsupported or duplicate time-zone identifier.".to_string());
    }
    Ok(Some(ids))
}

fn parse_availability_request(request: &str) -> std::result::Result<(String, i64, i64), String> {
    // [MS-OXWAVLS] §§3.1.4.1.3.13-.14 bounds this adapter to one mailbox/window.
    let mailbox_arrays = element_contents(request, "MailboxDataArray");
    if mailbox_arrays.len() != 1 {
        return Err("GetUserAvailability requires exactly one MailboxDataArray.".to_string());
    }
    let mailbox_data = element_contents(mailbox_arrays[0], "MailboxData");
    if mailbox_data.len() != 1 {
        return Err("LPE supports exactly one MailboxData entry per request.".to_string());
    }
    let email = element_contents(mailbox_data[0], "Email");
    let address = email
        .first()
        .and_then(|email| element_text(email, "Address"))
        .map(|address| address.trim().to_ascii_lowercase())
        .filter(|address| !address.is_empty())
        .ok_or_else(|| "MailboxData requires a non-empty Email Address.".to_string())?;
    let windows = element_contents(request, "TimeWindow");
    if windows.len() != 1 {
        return Err("FreeBusyViewOptions requires exactly one TimeWindow.".to_string());
    }
    let start = element_text(windows[0], "StartTime")
        .and_then(|value| ews_datetime_minutes(&value))
        .ok_or_else(|| "TimeWindow requires a valid StartTime.".to_string())?;
    let end = element_text(windows[0], "EndTime")
        .and_then(|value| ews_datetime_minutes(&value))
        .ok_or_else(|| "TimeWindow requires a valid EndTime.".to_string())?;
    if end <= start || end - start > 42 * 24 * 60 {
        return Err("TimeWindow must be positive and no longer than 42 days.".to_string());
    }
    Ok((address, start, end))
}

fn expand_availability_event(
    event: &AccessibleEvent,
    start: i64,
    end: i64,
) -> Vec<AccessibleEvent> {
    if event.recurrence_rule.trim().is_empty() {
        return vec![event.clone()];
    }
    let parts = event
        .recurrence_rule
        .split(';')
        .filter_map(|part| part.split_once('='))
        .collect::<std::collections::BTreeMap<_, _>>();
    let frequency = parts.get("FREQ").copied().unwrap_or_default();
    let interval = parts
        .get("INTERVAL")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let limit = parts
        .get("COUNT")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000)
        .min(1_000);
    let Some(anchor) = ews_datetime_minutes(&format!("{}T{}:00Z", event.date, event.time)) else {
        return Vec::new();
    };
    let mut occurrences = Vec::new();
    let mut cursor = anchor;
    for index in 0..limit {
        let (date, time) = ews_datetime_from_minutes(cursor);
        let mut occurrence = event.clone();
        occurrence.date = date;
        occurrence.time = time;
        if event_overlaps_window(&occurrence, start, end) {
            occurrences.push(occurrence);
        }
        cursor = match frequency {
            "DAILY" => cursor.saturating_add(interval * 1_440),
            "WEEKLY" => cursor.saturating_add(interval * 10_080),
            _ => break,
        };
        if index + 1 == limit {
            break;
        }
    }
    occurrences
}

fn availability_event_start_minutes(event: &AccessibleEvent) -> Option<i64> {
    let local = ews_datetime_minutes(&format!("{}T{}:00Z", event.date, event.time))?;
    let offset = match event.time_zone.as_str() {
        value if value.eq_ignore_ascii_case("UTC") || value.trim().is_empty() => 0,
        value if value.eq_ignore_ascii_case("Europe/Berlin") => {
            western_europe_offset_minutes(local)
        }
        _ => return None,
    };
    Some(local.saturating_sub(offset))
}

fn availability_event_start_datetime(event: &AccessibleEvent) -> String {
    availability_event_start_minutes(event)
        .map(ews_datetime_from_minutes)
        .map(|(date, time)| ews_datetime(&date, &time))
        .unwrap_or_else(|| ews_datetime(&event.date, &event.time))
}

fn availability_event_end_datetime(event: &AccessibleEvent) -> String {
    availability_event_start_minutes(event)
        .map(|start| start.saturating_add(i64::from(event.duration_minutes.max(0))))
        .map(ews_datetime_from_minutes)
        .map(|(date, time)| ews_datetime(&date, &time))
        .unwrap_or_else(|| event_end_datetime(event))
}

fn ews_datetime_minutes(value: &str) -> Option<i64> {
    let date = value.get(0..10)?;
    let time = value.get(11..16)?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let normalized_days = days_from_civil(year, month, day);
    let (normalized_year, normalized_month, normalized_day) = civil_from_days(normalized_days);
    (date_parts.next().is_none()
        && time_parts.next().is_none()
        && (1..=12).contains(&month)
        && (1..=31).contains(&day)
        && (year, month, day) == (normalized_year, normalized_month, normalized_day)
        && (0..24).contains(&hour)
        && (0..60).contains(&minute))
    .then(|| normalized_days * 1_440 + hour * 60 + minute)
}

fn ews_datetime_from_minutes(minutes: i64) -> (String, String) {
    let days = minutes.div_euclid(1_440);
    let minute_of_day = minutes.rem_euclid(1_440);
    let (year, month, day) = civil_from_days(days);
    (
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{:02}:{:02}", minute_of_day / 60, minute_of_day % 60),
    )
}

fn western_europe_offset_minutes(local_minutes: i64) -> i64 {
    let (year, _, _) = civil_from_days(local_minutes.div_euclid(1_440));
    let daylight_start = western_europe_transition_minutes(year, 3, 2);
    let standard_start = western_europe_transition_minutes(year, 10, 3);
    if (daylight_start..standard_start).contains(&local_minutes) {
        120
    } else {
        60
    }
}

fn western_europe_transition_minutes(year: i64, month: i64, hour: i64) -> i64 {
    let last_day = days_from_civil(year, month, 31);
    let sunday_offset = (last_day + 4).rem_euclid(7);
    (last_day - sunday_offset) * 1_440 + hour * 60
}
