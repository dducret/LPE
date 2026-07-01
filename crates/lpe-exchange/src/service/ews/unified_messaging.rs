use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn play_on_phone(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let phone_number = element_text(request, "DialString")
            .or_else(|| element_text(request, "PhoneNumber"))
            .or_else(|| element_text(request, "PhoneNumberString"))
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let message_id = requested_item_ids(request)
            .into_iter()
            .find_map(|id| canonical_message_id_from_ews_id(&id));
        match self
            .store
            .create_ews_unified_messaging_call(
                principal,
                phone_number.as_deref(),
                message_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-play-on-phone".to_string(),
                    subject: phone_number
                        .clone()
                        .unwrap_or_else(|| "default".to_string()),
                },
            )
            .await
        {
            Ok(call) => Ok(play_on_phone_response(&call)),
            Err(error) => Ok(operation_error_response(
                "PlayOnPhone",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )),
        }
    }

    pub(in crate::service) async fn get_phone_call_information(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(call_id) = phone_call_id_from_request(request) else {
            return Ok(operation_error_response(
                "GetPhoneCallInformation",
                "ErrorInvalidOperation",
                "GetPhoneCallInformation requires a PhoneCallId value.",
            ));
        };
        match self
            .store
            .fetch_ews_unified_messaging_call(principal, &call_id)
            .await?
        {
            Some(call) => Ok(phone_call_information_response(&call)),
            None => Ok(operation_error_response(
                "GetPhoneCallInformation",
                "ErrorItemNotFound",
                "Unified Messaging call was not found.",
            )),
        }
    }

    pub(in crate::service) async fn disconnect_phone_call(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(call_id) = phone_call_id_from_request(request) else {
            return Ok(operation_error_response(
                "DisconnectPhoneCall",
                "ErrorInvalidOperation",
                "DisconnectPhoneCall requires a PhoneCallId value.",
            ));
        };
        match self
            .store
            .disconnect_ews_unified_messaging_call(
                principal,
                &call_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-disconnect-phone-call".to_string(),
                    subject: call_id.clone(),
                },
            )
            .await?
        {
            Some(call) => Ok(disconnect_phone_call_response(&call)),
            None => Ok(operation_error_response(
                "DisconnectPhoneCall",
                "ErrorItemNotFound",
                "Unified Messaging call was not found or is already complete.",
            )),
        }
    }
}

pub(in crate::service) fn phone_call_id_from_request(request: &str) -> Option<String> {
    element_text(request, "PhoneCallId")
        .or_else(|| element_text(request, "CallId"))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

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
