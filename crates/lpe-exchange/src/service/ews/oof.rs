use super::super::*;

pub(in crate::service) fn get_user_oof_settings_response(projection: &OofProjection) -> String {
    let state = projection.state.as_ews();
    let audience = &projection.external_audience;
    let duration = if let Some(duration) = &projection.duration {
        format!(
            concat!(
                "<t:Duration>",
                "<t:StartTime>{start_time}</t:StartTime>",
                "<t:EndTime>{end_time}</t:EndTime>",
                "</t:Duration>"
            ),
            start_time = escape_xml(&duration.start_time),
            end_time = escape_xml(&duration.end_time),
        )
    } else {
        String::new()
    };
    let message = escape_xml(&projection.text_body);
    format!(
        concat!(
            "<m:GetUserOofSettingsResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<t:OofSettings>",
            "<t:OofState>{state}</t:OofState>",
            "<t:ExternalAudience>{audience}</t:ExternalAudience>",
            "{duration}",
            "<t:InternalReply><t:Message>{message}</t:Message></t:InternalReply>",
            "<t:ExternalReply><t:Message>{message}</t:Message></t:ExternalReply>",
            "</t:OofSettings>",
            "<m:AllowExternalOof>{audience}</m:AllowExternalOof>",
            "</m:GetUserOofSettingsResponse>"
        ),
        state = state,
        audience = audience,
        duration = duration,
        message = message,
    )
}

pub(in crate::service) fn set_user_oof_settings_success_response() -> String {
    concat!(
        "<m:SetUserOofSettingsResponse>",
        "<m:ResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:ResponseMessage>",
        "</m:SetUserOofSettingsResponse>"
    )
    .to_string()
}
