use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn find_message_tracking_report(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let query_text = message_tracking_query_text(request);
        let reports = self
            .store
            .fetch_ews_message_tracking_reports(principal, &query_text, 100)
            .await?;
        Ok(find_message_tracking_report_response(&reports))
    }

    pub(in crate::service) async fn get_message_tracking_report(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let report_id = requested_message_tracking_report_id(request);
        let Some(report_id) = report_id else {
            return Ok(operation_error_response(
                "GetMessageTrackingReport",
                "ErrorInvalidOperation",
                "MessageTrackingReportId is required.",
            ));
        };
        let detail = self
            .store
            .fetch_ews_message_tracking_report_detail(principal, &report_id)
            .await?;
        match detail {
            Some(detail) => Ok(get_message_tracking_report_response(&detail)),
            None => Ok(operation_error_response(
                "GetMessageTrackingReport",
                "ErrorItemNotFound",
                "The requested message tracking report was not found.",
            )),
        }
    }
}

pub(in crate::service) fn find_message_tracking_report_response(
    reports: &[EwsMessageTrackingReport],
) -> String {
    let reports_xml = reports
        .iter()
        .map(message_tracking_report_xml)
        .collect::<String>();
    format!(
        concat!(
            "<m:FindMessageTrackingReportResponse>",
            "<m:ResponseMessages>",
            "<m:FindMessageTrackingReportResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:MessageTrackingSearchResults>{reports_xml}</m:MessageTrackingSearchResults>",
            "</m:FindMessageTrackingReportResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindMessageTrackingReportResponse>"
        ),
        reports_xml = reports_xml,
    )
}

pub(in crate::service) fn get_message_tracking_report_response(
    detail: &EwsMessageTrackingReportDetail,
) -> String {
    let events_xml = detail
        .events
        .iter()
        .map(|event| {
            format!(
                concat!(
                    "<t:RecipientTrackingEvent>",
                    "<t:Date>{timestamp}</t:Date>",
                    "<t:EventDescription>{event_kind}</t:EventDescription>",
                    "<t:EventData>{event_source}</t:EventData>",
                    "<t:RecipientAddress>{recipient}</t:RecipientAddress>",
                    "<t:DeliveryStatus>{event_kind}</t:DeliveryStatus>",
                    "<t:DiagnosticInformation>{diagnostics}</t:DiagnosticInformation>",
                    "</t:RecipientTrackingEvent>"
                ),
                timestamp = escape_xml(&event.timestamp),
                event_kind = escape_xml(&event.event_kind),
                event_source = escape_xml(&event.event_source),
                recipient = escape_xml(event.recipient_address.as_deref().unwrap_or_default()),
                diagnostics = escape_xml(&event.dsn_json),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetMessageTrackingReportResponse>",
            "<m:ResponseMessages>",
            "<m:GetMessageTrackingReportResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:MessageTrackingReport>",
            "{report_xml}",
            "<t:RecipientTrackingEvents>{events_xml}</t:RecipientTrackingEvents>",
            "</m:MessageTrackingReport>",
            "</m:GetMessageTrackingReportResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetMessageTrackingReportResponse>"
        ),
        report_xml = message_tracking_report_xml(&detail.report),
        events_xml = events_xml,
    )
}

pub(in crate::service) fn message_tracking_query_text(request: &str) -> String {
    element_text(request, "MessageTrackingReportId")
        .or_else(|| element_text(request, "TraceId"))
        .or_else(|| element_text(request, "EmailAddress"))
        .or_else(|| element_text(request, "SmtpAddress"))
        .or_else(|| element_text(request, "Subject"))
        .or_else(|| element_text(request, "Query"))
        .or_else(|| element_text(request, "Sender"))
        .or_else(|| element_text(request, "Recipient"))
        .unwrap_or_default()
        .trim()
        .to_string()
}

pub(in crate::service) fn requested_message_tracking_report_id(request: &str) -> Option<String> {
    element_text(request, "MessageTrackingReportId")
        .or_else(|| element_text(request, "ReportId"))
        .or_else(|| element_text(request, "TraceId"))
        .or_else(|| {
            attribute_values_for_tag(request, "MessageTrackingReportId", "Id")
                .first()
                .map(|value| (*value).to_string())
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn message_tracking_report_xml(report: &EwsMessageTrackingReport) -> String {
    let recipients_xml = report
        .recipients
        .iter()
        .map(|recipient| format!("<t:SmtpAddress>{}</t:SmtpAddress>", escape_xml(recipient)))
        .collect::<String>();
    format!(
        concat!(
            "<t:MessageTrackingSearchResult>",
            "<t:MessageTrackingReportId>{report_id}</t:MessageTrackingReportId>",
            "<t:Sender>{sender}</t:Sender>",
            "<t:Recipients>{recipients_xml}</t:Recipients>",
            "<t:Subject>{subject}</t:Subject>",
            "<t:SubmittedTime>{submitted_at}</t:SubmittedTime>",
            "<t:Status>{status}</t:Status>",
            "<t:TraceId>{trace_id}</t:TraceId>",
            "<t:RemoteMessageReference>{remote_ref}</t:RemoteMessageReference>",
            "</t:MessageTrackingSearchResult>"
        ),
        report_id = escape_xml(&report.report_id),
        sender = escape_xml(&report.sender),
        recipients_xml = recipients_xml,
        subject = escape_xml(&report.subject),
        submitted_at = escape_xml(&report.submitted_at),
        status = escape_xml(&report.status),
        trace_id = escape_xml(report.trace_id.as_deref().unwrap_or_default()),
        remote_ref = escape_xml(report.remote_message_ref.as_deref().unwrap_or_default()),
    )
}
