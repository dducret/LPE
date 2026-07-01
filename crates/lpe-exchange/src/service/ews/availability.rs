use super::super::*;

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
        if let Some(mailbox) = element_content(request, "MailboxData")
            .and_then(|mailbox_data| element_content(mailbox_data, "Email"))
            .and_then(parse_mailbox)
        {
            if !mailbox.address.eq_ignore_ascii_case(&principal.email) {
                return Ok(get_user_availability_error_response(
                    "Free/busy is currently available only for the authenticated mailbox.",
                ));
            }
        }

        let (window_start, window_end) = requested_availability_window(request);
        let mut events = self
            .store
            .fetch_accessible_events_in_collection(principal.account_id, DEFAULT_COLLECTION_ID)
            .await?;
        events.retain(|event| {
            event_overlaps_window(event, window_start.as_deref(), window_end.as_deref())
        });
        events.sort_by(|left, right| {
            ews_datetime(&left.date, &left.time).cmp(&ews_datetime(&right.date, &right.time))
        });
        Ok(get_user_availability_success_response(
            &events,
            availability_suggestions_response(request).as_deref(),
        ))
    }
}

pub(in crate::service) fn get_server_time_zones_response() -> String {
    concat!(
        "<m:GetServerTimeZonesResponse>",
        "<m:ResponseMessages>",
        "<m:GetServerTimeZonesResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "<m:TimeZoneDefinitions>",
        "<t:TimeZoneDefinition Id=\"UTC\" Name=\"(UTC) Coordinated Universal Time\"/>",
        "<t:TimeZoneDefinition Id=\"W. Europe Standard Time\" Name=\"(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna\"/>",
        "</m:TimeZoneDefinitions>",
        "</m:GetServerTimeZonesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:GetServerTimeZonesResponse>"
    )
    .to_string()
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
                escape_xml(&ews_datetime(&event.date, &event.time)),
                escape_xml(&event_end_datetime(event)),
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
        .or_else(|| requested_availability_window(request).0)
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

pub(in crate::service) fn requested_availability_window(
    request: &str,
) -> (Option<String>, Option<String>) {
    let time_window = element_content(request, "TimeWindow").unwrap_or(request);
    (
        element_text(time_window, "StartTime"),
        element_text(time_window, "EndTime"),
    )
}

pub(in crate::service) fn event_overlaps_window(
    event: &AccessibleEvent,
    start: Option<&str>,
    end: Option<&str>,
) -> bool {
    let event_start = ews_datetime(&event.date, &event.time);
    let event_end = event_end_datetime(event);
    start.is_none_or(|start| event_end.as_str() > start)
        && end.is_none_or(|end| event_start.as_str() < end)
}
