use super::super::*;

pub(in crate::service) fn play_on_phone_response(call: &EwsUnifiedMessagingCall) -> String {
    format!(
        concat!(
            "<m:PlayOnPhoneResponse>",
            "<m:ResponseMessages>",
            "<m:PlayOnPhoneResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{call_xml}",
            "</m:PlayOnPhoneResponseMessage>",
            "</m:ResponseMessages>",
            "</m:PlayOnPhoneResponse>"
        ),
        call_xml = unified_messaging_call_xml(call),
    )
}

pub(in crate::service) fn phone_call_information_response(
    call: &EwsUnifiedMessagingCall,
) -> String {
    format!(
        concat!(
            "<m:GetPhoneCallInformationResponse>",
            "<m:ResponseMessages>",
            "<m:GetPhoneCallInformationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{call_xml}",
            "</m:GetPhoneCallInformationResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetPhoneCallInformationResponse>"
        ),
        call_xml = unified_messaging_call_xml(call),
    )
}

pub(in crate::service) fn disconnect_phone_call_response(call: &EwsUnifiedMessagingCall) -> String {
    format!(
        concat!(
            "<m:DisconnectPhoneCallResponse>",
            "<m:ResponseMessages>",
            "<m:DisconnectPhoneCallResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{call_xml}",
            "</m:DisconnectPhoneCallResponseMessage>",
            "</m:ResponseMessages>",
            "</m:DisconnectPhoneCallResponse>"
        ),
        call_xml = unified_messaging_call_xml(call),
    )
}

fn unified_messaging_call_xml(call: &EwsUnifiedMessagingCall) -> String {
    let phone_number = call
        .phone_number
        .as_ref()
        .map(|value| format!("<t:PhoneNumber>{}</t:PhoneNumber>", escape_xml(value)))
        .unwrap_or_default();
    let message_id = call
        .message_id
        .map(|value| format!("<t:ItemId Id=\"message:{value}\"/>"))
        .unwrap_or_default();
    format!(
        concat!(
            "<m:PhoneCallInformation>",
            "<t:PhoneCallId>{call_id}</t:PhoneCallId>",
            "<t:CallKind>{call_kind}</t:CallKind>",
            "<t:CallState>{status}</t:CallState>",
            "{phone_number}",
            "{message_id}",
            "<t:RequestedAt>{requested_at}</t:RequestedAt>",
            "<t:UpdatedAt>{updated_at}</t:UpdatedAt>",
            "</m:PhoneCallInformation>"
        ),
        call_id = escape_xml(&call.call_id),
        call_kind = escape_xml(&call.call_kind),
        status = escape_xml(&call.status),
        phone_number = phone_number,
        message_id = message_id,
        requested_at = escape_xml(&call.requested_at),
        updated_at = escape_xml(&call.updated_at),
    )
}
