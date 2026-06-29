use super::super::*;

pub(in crate::service) fn get_inbox_rules_response(rules: &[MailboxRule]) -> String {
    let mut rules_xml = String::new();
    for (index, rule) in rules.iter().enumerate() {
        rules_xml.push_str(&format!(
            concat!(
                "<t:Rule>",
                "<t:RuleId>{id}</t:RuleId>",
                "<t:DisplayName>{name}</t:DisplayName>",
                "<t:Priority>{priority}</t:Priority>",
                "<t:IsEnabled>{enabled}</t:IsEnabled>",
                "<t:IsNotSupported>{unsupported}</t:IsNotSupported>",
                "<t:IsInError>false</t:IsInError>",
                "</t:Rule>"
            ),
            id = escape_xml(&rule.name),
            name = escape_xml(&rule.name),
            priority = index + 1,
            enabled = if rule.is_active { "true" } else { "false" },
            unsupported = if rule.supported_outlook_projection {
                "false"
            } else {
                "true"
            },
        ));
    }
    format!(
        concat!(
            "<m:GetInboxRulesResponse>",
            "<m:ResponseMessages>",
            "<m:GetInboxRulesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:OutlookRuleBlobExists>false</m:OutlookRuleBlobExists>",
            "<m:InboxRules>{rules_xml}</m:InboxRules>",
            "</m:GetInboxRulesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetInboxRulesResponse>"
        ),
        rules_xml = rules_xml
    )
}
