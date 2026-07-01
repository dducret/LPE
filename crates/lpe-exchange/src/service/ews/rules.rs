use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_inbox_rules(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let rules = self.store.list_mailbox_rules(principal.account_id).await?;
        Ok(get_inbox_rules_response(&rules))
    }

    pub(in crate::service) async fn update_inbox_rules(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let mut mutations = Vec::new();
            for operation in element_contents(request, "DeleteRuleOperation") {
                let rule_id = element_text(operation, "RuleId")
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| anyhow!("DeleteRuleOperation requires RuleId."))?;
                mutations.push(EwsInboxRuleMutation::Delete { rule_id });
            }
            for operation in element_contents(request, "CreateRuleOperation") {
                let rule = element_content(operation, "Rule").unwrap_or(operation);
                let (name, active, sieve) = bounded_ews_rule_to_sieve(rule)?;
                mutations.push(EwsInboxRuleMutation::Put {
                    name,
                    active,
                    sieve,
                    audit_action: "ews-update-inbox-rules-create",
                });
            }
            for operation in element_contents(request, "SetRuleOperation") {
                let rule = element_content(operation, "Rule").unwrap_or(operation);
                let (name, active, sieve) = bounded_ews_rule_to_sieve(rule)?;
                mutations.push(EwsInboxRuleMutation::Put {
                    name,
                    active,
                    sieve,
                    audit_action: "ews-update-inbox-rules-set",
                });
            }
            if mutations.is_empty() && !request.contains("RemoveOutlookRuleBlob") {
                bail!("UpdateInboxRules supports bounded create, set, and delete rule operations.");
            }

            for mutation in mutations {
                match mutation {
                    EwsInboxRuleMutation::Delete { rule_id } => {
                        self.store
                            .delete_sieve_script(
                                principal.account_id,
                                &rule_id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-update-inbox-rules-delete".to_string(),
                                    subject: rule_id.clone(),
                                },
                            )
                            .await?;
                    }
                    EwsInboxRuleMutation::Put {
                        name,
                        active,
                        sieve,
                        audit_action,
                    } => {
                        self.store
                            .put_sieve_script(
                                principal.account_id,
                                &name,
                                &sieve,
                                active,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: audit_action.to_string(),
                                    subject: name.clone(),
                                },
                            )
                            .await?;
                    }
                }
            }
            Ok(simple_operation_success_response("UpdateInboxRules"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "UpdateInboxRules",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }
}

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
