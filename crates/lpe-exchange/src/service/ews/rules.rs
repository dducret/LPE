use super::super::*;

#[derive(Debug, Clone)]
pub(in crate::service) enum EwsInboxRuleMutation {
    Delete {
        rule_id: String,
    },
    Put {
        name: String,
        active: bool,
        sieve: String,
        audit_action: &'static str,
    },
}

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

pub(in crate::service) fn bounded_ews_rule_to_sieve(rule: &str) -> Result<(String, bool, String)> {
    if element_text(rule, "IsClientOnly")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        bail!("UpdateInboxRules does not support client-only Exchange rules.");
    }
    if rule.contains("RuleProviderData")
        || rule.contains("RuleBlob")
        || rule.contains("DeferredAction")
        || rule.contains("DeferredActionMessage")
    {
        bail!("UpdateInboxRules does not support Exchange rule blobs or deferred-action data.");
    }

    let name = element_text(rule, "DisplayName")
        .filter(|value| !value.trim().is_empty())
        .or_else(|| element_text(rule, "RuleId").filter(|value| !value.trim().is_empty()))
        .unwrap_or_else(|| format!("ews-rule-{}", Uuid::new_v4()));
    let active = element_text(rule, "IsEnabled")
        .map(|value| !value.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let subject = element_content(rule, "SubjectContainsWords")
        .and_then(|content| element_text(content, "String"))
        .filter(|value| !value.trim().is_empty());
    let target = element_content(rule, "MoveToFolder")
        .and_then(|content| {
            element_text(content, "DisplayName")
                .or_else(|| element_text(content, "Name"))
                .or_else(|| attribute_value_after(content, "FolderId", "Id").map(str::to_string))
        })
        .unwrap_or_else(|| "Inbox".to_string());
    let sieve = if let Some(subject) = subject {
        format!(
            concat!(
                "require [\"fileinto\"];\n",
                "if header :contains \"Subject\" \"{subject}\" {{\n",
                "  fileinto \"{target}\";\n",
                "  stop;\n",
                "}}\n"
            ),
            subject = escape_sieve_string(&subject),
            target = escape_sieve_string(&target),
        )
    } else if rule.contains("<t:Delete") || rule.contains("<Delete") {
        concat!(
            "require [\"discard\"];\n",
            "if true {\n",
            "  discard;\n",
            "  stop;\n",
            "}\n"
        )
        .to_string()
    } else {
        bail!("UpdateInboxRules supports subject contains with move-to-folder or delete.");
    };
    Ok((name, active, sieve))
}

fn escape_sieve_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
