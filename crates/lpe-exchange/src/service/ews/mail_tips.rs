use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_service_configuration(
        &self,
        request: &str,
    ) -> Result<String> {
        let configs = requested_service_configurations(request);
        Ok(get_service_configuration_response(&configs))
    }
}

#[derive(Debug, Clone)]
pub(in crate::service) struct MailTipProjection {
    pub(in crate::service) recipient: String,
    pub(in crate::service) display_name: Option<String>,
    pub(in crate::service) recipient_type: Option<ExchangeAddressBookEntryKind>,
    pub(in crate::service) invalid_recipient: bool,
    pub(in crate::service) out_of_office_message: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum RequestedServiceConfiguration {
    MailTips,
    UnifiedMessaging,
    ProtectionRules,
    PolicyTips,
    Unsupported,
}

pub(in crate::service) fn get_mail_tips_response(tips: &[MailTipProjection]) -> String {
    let tips_xml = tips.iter().map(mail_tip_xml).collect::<String>();
    format!(
        concat!(
            "<m:GetMailTipsResponse>",
            "<m:ResponseMessages>",
            "<m:GetMailTipsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode></m:ResponseMessage>",
            "<m:MailTips>{tips_xml}</m:MailTips>",
            "</m:GetMailTipsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetMailTipsResponse>"
        ),
        tips_xml = tips_xml,
    )
}

pub(in crate::service) fn get_service_configuration_response(
    configs: &[RequestedServiceConfiguration],
) -> String {
    let response_messages = configs
        .iter()
        .map(|config| match config {
            RequestedServiceConfiguration::MailTips => service_configuration_success_message(
                "MailTips",
                &format!(
                    concat!(
                        "<m:MailTipsConfiguration>",
                        "<t:MailTipsEnabled>true</t:MailTipsEnabled>",
                        "<t:MaxRecipientsPerGetMailTipsRequest>{}</t:MaxRecipientsPerGetMailTipsRequest>",
                        "<t:MaxMessageSize>0</t:MaxMessageSize>",
                        "<t:LargeAudienceThreshold>0</t:LargeAudienceThreshold>",
                        "</m:MailTipsConfiguration>"
                    ),
                    EWS_MAX_MAIL_TIPS_RECIPIENTS
                ),
            ),
            RequestedServiceConfiguration::UnifiedMessaging => service_configuration_error_message(
                "UnifiedMessagingConfiguration",
                "Unified Messaging service configuration is not implemented by LPE.",
            ),
            RequestedServiceConfiguration::ProtectionRules => service_configuration_error_message(
                "ProtectionRules",
                "Protection Rules service configuration is not implemented by LPE.",
            ),
            RequestedServiceConfiguration::PolicyTips => service_configuration_error_message(
                "PolicyTips",
                "Policy Tips service configuration is not implemented by LPE.",
            ),
            RequestedServiceConfiguration::Unsupported => service_configuration_error_message(
                "Unknown",
                "The requested service configuration is not implemented by LPE.",
            ),
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetServiceConfigurationResponse>",
            "<m:ResponseMessages>{response_messages}</m:ResponseMessages>",
            "</m:GetServiceConfigurationResponse>"
        ),
        response_messages = response_messages,
    )
}

pub(in crate::service) fn requested_mail_tips_recipients(request: &str) -> Vec<String> {
    element_content(request, "RecipientMailboxes")
        .or_else(|| element_content(request, "Recipients"))
        .map(|mailboxes| {
            element_contents(mailboxes, "Mailbox")
                .into_iter()
                .filter_map(parse_mailbox)
                .map(|mailbox| mailbox.address.trim().to_ascii_lowercase())
                .filter(|address| !address.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

pub(in crate::service) fn requested_mail_tips(request: &str) -> HashSet<String> {
    element_content(request, "MailTipsRequested")
        .map(|value| {
            value
                .split(|ch: char| ch.is_whitespace() || ch == ',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| {
            ["InvalidRecipient", "OutOfOfficeMessage"]
                .into_iter()
                .map(str::to_string)
                .collect()
        })
}

pub(in crate::service) fn requested_service_configurations(
    request: &str,
) -> Vec<RequestedServiceConfiguration> {
    let mut configs = Vec::new();
    let mut values = element_contents(request, "ConfigurationName")
        .into_iter()
        .map(xml_text)
        .collect::<Vec<_>>();
    if values.is_empty() {
        values.extend(
            element_text(request, "RequestedConfiguration")
                .into_iter()
                .flat_map(|value| {
                    value
                        .split(|ch: char| ch.is_whitespace() || ch == ',')
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                }),
        );
    }
    for value in values {
        let normalized = value.trim();
        if normalized.is_empty() {
            continue;
        }
        let config = match normalized {
            "MailTips" | "MailTipsConfiguration" => RequestedServiceConfiguration::MailTips,
            "UnifiedMessaging" | "UnifiedMessagingConfiguration" => {
                RequestedServiceConfiguration::UnifiedMessaging
            }
            "ProtectionRules" | "ProtectionRulesConfiguration" => {
                RequestedServiceConfiguration::ProtectionRules
            }
            "PolicyTips" | "PolicyTipsConfiguration" => RequestedServiceConfiguration::PolicyTips,
            _ => RequestedServiceConfiguration::Unsupported,
        };
        if !configs.contains(&config) {
            configs.push(config);
        }
    }
    if configs.is_empty() {
        configs.push(RequestedServiceConfiguration::MailTips);
    }
    configs
}

fn service_configuration_success_message(configuration_name: &str, payload: &str) -> String {
    format!(
        concat!(
            "<m:GetServiceConfigurationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ConfigurationName>{configuration_name}</m:ConfigurationName>",
            "{payload}",
            "</m:GetServiceConfigurationResponseMessage>"
        ),
        configuration_name = escape_xml(configuration_name),
        payload = payload,
    )
}

fn service_configuration_error_message(configuration_name: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetServiceConfigurationResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>",
            "<m:ConfigurationName>{configuration_name}</m:ConfigurationName>",
            "</m:GetServiceConfigurationResponseMessage>"
        ),
        configuration_name = escape_xml(configuration_name),
        message = escape_xml(message),
    )
}

fn mail_tip_xml(tip: &MailTipProjection) -> String {
    let mailbox_type = tip
        .recipient_type
        .map(|kind| match kind {
            ExchangeAddressBookEntryKind::Account => "Mailbox",
            ExchangeAddressBookEntryKind::Contact => "Contact",
            ExchangeAddressBookEntryKind::DistributionList => "PublicDL",
        })
        .unwrap_or("Unknown");
    let display_name = tip.display_name.as_deref().unwrap_or(&tip.recipient);
    let invalid = if tip.invalid_recipient {
        "<t:InvalidRecipient>true</t:InvalidRecipient>"
    } else {
        "<t:InvalidRecipient>false</t:InvalidRecipient>"
    };
    let oof = tip
        .out_of_office_message
        .as_deref()
        .map(|message| {
            format!(
                concat!(
                    "<t:OutOfOffice>",
                    "<t:ReplyBody><t:Message>{message}</t:Message></t:ReplyBody>",
                    "</t:OutOfOffice>"
                ),
                message = escape_xml(message),
            )
        })
        .unwrap_or_else(|| "<t:OutOfOffice/>".to_string());
    format!(
        concat!(
            "<t:MailTips>",
            "<t:RecipientAddress>",
            "<t:Name>{name}</t:Name>",
            "<t:EmailAddress>{email}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "<t:MailboxType>{mailbox_type}</t:MailboxType>",
            "</t:RecipientAddress>",
            "{invalid}",
            "{oof}",
            "</t:MailTips>"
        ),
        name = escape_xml(display_name),
        email = escape_xml(&tip.recipient),
        mailbox_type = mailbox_type,
        invalid = invalid,
        oof = oof,
    )
}
