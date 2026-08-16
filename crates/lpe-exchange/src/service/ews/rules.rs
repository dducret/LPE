use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

use super::super::*;

// [MS-OXWSRULES] §§2.2.4.1, 2.2.4.4-.5, 3.1.4.1, 3.1.4.2.
const EWS_INBOX_RULES_SCRIPT_NAME: &str = "lpe-ews-inbox-rules-v1";
const EWS_INBOX_RULES_MARKER: &str = "# lpe-ews-inbox-rules-v1";
const EWS_INBOX_RULE_MARKER: &str = "# lpe-ews-rule-v1";

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_inbox_rules(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        validate_rules_mailbox(principal, request)?;
        let rules = match self
            .store
            .fetch_active_sieve_script(principal.account_id)
            .await?
        {
            Some(script) => parse_ews_inbox_rule_script(&script.content)?.unwrap_or_default(),
            None => Vec::new(),
        };
        Ok(get_inbox_rules_response(&rules))
    }

    pub(in crate::service) async fn update_inbox_rules(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            validate_rules_mailbox(principal, request)?;
            let mutations = parse_rule_mutations(request)?;
            let active_script = self
                .store
                .fetch_active_sieve_script(principal.account_id)
                .await?;
            let (expected_content, mut rules) = match active_script {
                Some(script) => {
                    let rules = parse_ews_inbox_rule_script(&script.content)?.ok_or_else(|| {
                        anyhow!(
                            "the active canonical Sieve script is not the bounded EWS inbox-rule projection"
                        )
                    })?;
                    (Some(script.content), rules)
                }
                None => (None, Vec::new()),
            };
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            apply_rule_mutations(&mut rules, &mutations, &mailboxes)?;
            let replacement = (!rules.is_empty()).then(|| render_ews_inbox_rule_script(&rules));
            if replacement.as_deref() != expected_content.as_deref() {
                self.store
                    .replace_active_sieve_script(
                        principal.account_id,
                        EWS_INBOX_RULES_SCRIPT_NAME,
                        expected_content.as_deref(),
                        replacement.as_deref(),
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-update-inbox-rules".to_string(),
                            subject: format!("{} rule operations", mutations.len()),
                        },
                    )
                    .await?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct EwsInboxRule {
    id: Uuid,
    name: String,
    enabled: bool,
    subject: String,
    action: EwsInboxRuleAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EwsInboxRuleAction {
    MoveToFolder { id: Uuid, name: String },
    Delete,
}

#[derive(Debug, Clone)]
enum EwsInboxRuleMutation {
    Create(EwsInboxRuleInput),
    Set { id: Uuid, input: EwsInboxRuleInput },
    Delete { id: Uuid },
}

#[derive(Debug, Clone)]
struct EwsInboxRuleInput {
    name: String,
    enabled: bool,
    subject: String,
    action: EwsInboxRuleActionInput,
}

#[derive(Debug, Clone)]
enum EwsInboxRuleActionInput {
    MoveToFolder(Uuid),
    Delete,
}

pub(in crate::service) fn get_inbox_rules_response(rules: &[EwsInboxRule]) -> String {
    let rules_xml = rules
        .iter()
        .enumerate()
        .map(|(index, rule)| {
            let action = match &rule.action {
                EwsInboxRuleAction::MoveToFolder { id, .. } => format!(
                    "<t:Actions><t:MoveToFolder><t:FolderId Id=\"mailbox:{id}\"/></t:MoveToFolder></t:Actions>"
                ),
                EwsInboxRuleAction::Delete => "<t:Actions><t:Delete/></t:Actions>".to_string(),
            };
            format!(
                concat!(
                    "<t:Rule>",
                    "<t:RuleId>{id}</t:RuleId>",
                    "<t:DisplayName>{name}</t:DisplayName>",
                    "<t:Priority>{priority}</t:Priority>",
                    "<t:IsEnabled>{enabled}</t:IsEnabled>",
                    "<t:IsNotSupported>false</t:IsNotSupported>",
                    "<t:IsInError>false</t:IsInError>",
                    "<t:Conditions><t:SubjectContainsWords><t:String>{subject}</t:String></t:SubjectContainsWords></t:Conditions>",
                    "{action}",
                    "</t:Rule>"
                ),
                id = ews_rule_id(rule.id),
                name = escape_xml(&rule.name),
                priority = index + 1,
                enabled = if rule.enabled { "true" } else { "false" },
                subject = escape_xml(&rule.subject),
                action = action,
            )
        })
        .collect::<String>();
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

fn validate_rules_mailbox(principal: &AccountPrincipal, request: &str) -> Result<()> {
    let addresses = element_contents(request, "MailboxSmtpAddress");
    if addresses.len() > 1 {
        bail!("Inbox rule requests accept at most one MailboxSmtpAddress.");
    }
    if let Some(address) = addresses.first().map(|value| xml_text(value)) {
        if address.is_empty() || !address.eq_ignore_ascii_case(&principal.email) {
            bail!("Inbox rules are available only for the authenticated mailbox.");
        }
    }
    Ok(())
}

fn parse_rule_mutations(request: &str) -> Result<Vec<EwsInboxRuleMutation>> {
    if !element_contents(request, "RemoveOutlookRuleBlob").is_empty() {
        bail!("UpdateInboxRules does not support Outlook rule blobs.");
    }
    let operations = exactly_one_content(request, "Operations")?;
    let mut found = Vec::new();
    for (kind, parser) in [
        (
            "CreateRuleOperation",
            parse_create_rule_operation as fn(&str) -> Result<EwsInboxRuleMutation>,
        ),
        ("SetRuleOperation", parse_set_rule_operation),
        ("DeleteRuleOperation", parse_delete_rule_operation),
    ] {
        for operation in element_contents(operations, kind) {
            found.push((operation.as_ptr() as usize, parser(operation)?));
        }
    }
    if found.is_empty() {
        bail!("UpdateInboxRules requires one or more bounded rule operations.");
    }
    if opening_rule_operation_names(operations).iter().any(|name| {
        !matches!(
            name.as_str(),
            "CreateRuleOperation" | "SetRuleOperation" | "DeleteRuleOperation"
        )
    }) {
        bail!("UpdateInboxRules contains an unsupported rule operation.");
    }
    found.sort_by_key(|(position, _)| *position);
    Ok(found.into_iter().map(|(_, mutation)| mutation).collect())
}

fn parse_create_rule_operation(operation: &str) -> Result<EwsInboxRuleMutation> {
    let rule = exactly_one_content(operation, "Rule")?;
    if !element_contents(rule, "RuleId").is_empty() {
        bail!("CreateRuleOperation must not specify RuleId.");
    }
    Ok(EwsInboxRuleMutation::Create(parse_ews_rule_input(rule)?))
}

fn parse_set_rule_operation(operation: &str) -> Result<EwsInboxRuleMutation> {
    let rule = exactly_one_content(operation, "Rule")?;
    let id = parse_ews_rule_id(&exactly_one_text(rule, "RuleId")?)?;
    Ok(EwsInboxRuleMutation::Set {
        id,
        input: parse_ews_rule_input(rule)?,
    })
}

fn parse_delete_rule_operation(operation: &str) -> Result<EwsInboxRuleMutation> {
    if !element_contents(operation, "Rule").is_empty() {
        bail!("DeleteRuleOperation must contain only RuleId.");
    }
    Ok(EwsInboxRuleMutation::Delete {
        id: parse_ews_rule_id(&exactly_one_text(operation, "RuleId")?)?,
    })
}

fn parse_ews_rule_input(rule: &str) -> Result<EwsInboxRuleInput> {
    reject_unsupported_rule_shape(rule)?;
    let name = bounded_rule_text(&exactly_one_text(rule, "DisplayName")?, "DisplayName")?;
    let enabled = match element_contents(rule, "IsEnabled").as_slice() {
        [] => true,
        [value] => match xml_text(value).as_str() {
            "true" | "True" => true,
            "false" | "False" => false,
            _ => bail!("IsEnabled must be true or false."),
        },
        _ => bail!("Rule must contain at most one IsEnabled value."),
    };
    let conditions = exactly_one_content(rule, "Conditions")?;
    let subject = bounded_rule_text(
        &exactly_one_text(
            exactly_one_content(conditions, "SubjectContainsWords")?,
            "String",
        )?,
        "SubjectContainsWords",
    )?;
    let actions = exactly_one_content(rule, "Actions")?;
    let moves = element_contents(actions, "MoveToFolder");
    let deletes = element_contents(actions, "Delete");
    let action = match (moves.as_slice(), deletes.as_slice()) {
        ([move_to_folder], []) => {
            EwsInboxRuleActionInput::MoveToFolder(parse_move_target(move_to_folder)?)
        }
        ([], [_]) => EwsInboxRuleActionInput::Delete,
        _ => bail!("Rule must contain exactly one supported action."),
    };
    Ok(EwsInboxRuleInput {
        name,
        enabled,
        subject,
        action,
    })
}

fn reject_unsupported_rule_shape(rule: &str) -> Result<()> {
    for prohibited in [
        "IsClientOnly",
        "RuleProviderData",
        "RuleBlob",
        "DeferredAction",
        "DeferredActionMessage",
    ] {
        if !element_contents(rule, prohibited).is_empty() {
            bail!("UpdateInboxRules does not support {prohibited}.");
        }
    }
    let allowed = [
        "RuleId",
        "DisplayName",
        "IsEnabled",
        "Conditions",
        "SubjectContainsWords",
        "String",
        "Actions",
        "MoveToFolder",
        "FolderId",
        "Delete",
    ];
    if opening_element_names(rule)
        .iter()
        .any(|name| !allowed.contains(&name.as_str()))
    {
        bail!("UpdateInboxRules contains an unsupported rule shape.");
    }
    Ok(())
}

fn parse_move_target(move_to_folder: &str) -> Result<Uuid> {
    if element_contents(move_to_folder, "FolderId").len() != 1
        || element_contents(move_to_folder, "DisplayName").len() != 0
        || element_contents(move_to_folder, "Name").len() != 0
    {
        bail!("MoveToFolder requires exactly one canonical mailbox FolderId.");
    }
    let id = attribute_value_after(move_to_folder, "FolderId", "Id")
        .ok_or_else(|| anyhow!("MoveToFolder requires FolderId Id."))?;
    let mailbox_id = id
        .strip_prefix("mailbox:")
        .ok_or_else(|| anyhow!("MoveToFolder supports canonical mailbox FolderId values only."))?;
    Uuid::parse_str(mailbox_id).map_err(|_| anyhow!("MoveToFolder mailbox FolderId is malformed."))
}

fn apply_rule_mutations(
    rules: &mut Vec<EwsInboxRule>,
    mutations: &[EwsInboxRuleMutation],
    mailboxes: &[JmapMailbox],
) -> Result<()> {
    let mut touched = std::collections::HashSet::new();
    for mutation in mutations {
        match mutation {
            EwsInboxRuleMutation::Create(input) => {
                rules.push(resolve_rule_input(Uuid::new_v4(), input, mailboxes)?)
            }
            EwsInboxRuleMutation::Set { id, input } => {
                if !touched.insert(*id) {
                    bail!("UpdateInboxRules cannot change the same RuleId more than once.");
                }
                let existing = rules
                    .iter_mut()
                    .find(|rule| rule.id == *id)
                    .ok_or_else(|| anyhow!("Inbox rule was not found."))?;
                *existing = resolve_rule_input(*id, input, mailboxes)?;
            }
            EwsInboxRuleMutation::Delete { id } => {
                if !touched.insert(*id) {
                    bail!("UpdateInboxRules cannot change the same RuleId more than once.");
                }
                let index = rules
                    .iter()
                    .position(|rule| rule.id == *id)
                    .ok_or_else(|| anyhow!("Inbox rule was not found."))?;
                rules.remove(index);
            }
        }
    }
    Ok(())
}

fn resolve_rule_input(
    id: Uuid,
    input: &EwsInboxRuleInput,
    mailboxes: &[JmapMailbox],
) -> Result<EwsInboxRule> {
    let action = match input.action {
        EwsInboxRuleActionInput::MoveToFolder(folder_id) => {
            let mailbox = mailbox_by_id(mailboxes, folder_id)?;
            ensure_custom_mailbox(mailbox)?;
            EwsInboxRuleAction::MoveToFolder {
                id: folder_id,
                name: mailbox.name.clone(),
            }
        }
        EwsInboxRuleActionInput::Delete => EwsInboxRuleAction::Delete,
    };
    Ok(EwsInboxRule {
        id,
        name: input.name.clone(),
        enabled: input.enabled,
        subject: input.subject.clone(),
        action,
    })
}

fn parse_ews_inbox_rule_script(content: &str) -> Result<Option<Vec<EwsInboxRule>>> {
    if !content.starts_with(EWS_INBOX_RULES_MARKER) {
        return Ok(None);
    }
    lpe_core::sieve::parse_script(content)?;
    let lines = content.lines().collect::<Vec<_>>();
    let mut rules = Vec::new();
    for (index, line) in lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with(EWS_INBOX_RULE_MARKER))
    {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 6 || fields[0] != "#" || fields[1] != "lpe-ews-rule-v1" {
            return Ok(None);
        }
        let Some(id) = Uuid::parse_str(fields[2]).ok() else {
            return Ok(None);
        };
        let enabled = match fields[3] {
            "1" => true,
            "0" => false,
            _ => return Ok(None),
        };
        let (Some(name), Some(subject)) =
            (decode_rule_value(fields[4]), decode_rule_value(fields[5]))
        else {
            return Ok(None);
        };
        let Some(action_line) = lines.get(index + 1) else {
            return Ok(None);
        };
        let action = if action_line.starts_with("# lpe-ews-action-move ") {
            let fields = action_line.split_whitespace().collect::<Vec<_>>();
            if fields.len() != 4 {
                return Ok(None);
            }
            match (
                Uuid::parse_str(fields[2]).ok(),
                decode_rule_value(fields[3]),
            ) {
                (Some(id), Some(name)) => EwsInboxRuleAction::MoveToFolder { id, name },
                _ => return Ok(None),
            }
        } else if *action_line == "# lpe-ews-action-delete" {
            EwsInboxRuleAction::Delete
        } else {
            return Ok(None);
        };
        rules.push(EwsInboxRule {
            id,
            name,
            enabled,
            subject,
            action,
        });
    }
    if rules.is_empty() || render_ews_inbox_rule_script(&rules) != content {
        return Ok(None);
    }
    Ok(Some(rules))
}

fn render_ews_inbox_rule_script(rules: &[EwsInboxRule]) -> String {
    let needs_fileinto = rules
        .iter()
        .any(|rule| rule.enabled && matches!(rule.action, EwsInboxRuleAction::MoveToFolder { .. }));
    let needs_discard = rules
        .iter()
        .any(|rule| rule.enabled && matches!(rule.action, EwsInboxRuleAction::Delete));
    let mut requirements = Vec::new();
    if needs_fileinto {
        requirements.push("\"fileinto\"");
    }
    if needs_discard {
        requirements.push("\"discard\"");
    }
    let mut output = format!("{EWS_INBOX_RULES_MARKER}\n");
    if !requirements.is_empty() {
        output.push_str(&format!("require [{}];\n", requirements.join(", ")));
    }
    for rule in rules {
        output.push_str(&format!(
            "{EWS_INBOX_RULE_MARKER} {} {} {} {}\n",
            rule.id,
            if rule.enabled { "1" } else { "0" },
            encode_rule_value(&rule.name),
            encode_rule_value(&rule.subject)
        ));
        match &rule.action {
            EwsInboxRuleAction::MoveToFolder { id, name } => output.push_str(&format!(
                "# lpe-ews-action-move {id} {}\n",
                encode_rule_value(name)
            )),
            EwsInboxRuleAction::Delete => output.push_str("# lpe-ews-action-delete\n"),
        }
        if !rule.enabled {
            continue;
        }
        match &rule.action {
            EwsInboxRuleAction::MoveToFolder { name, .. } => output.push_str(&format!(
                "if header :contains \"Subject\" \"{}\" {{\n  fileinto \"{}\";\n  stop;\n}}\n",
                escape_sieve_string(&rule.subject),
                escape_sieve_string(name)
            )),
            EwsInboxRuleAction::Delete => output.push_str(&format!(
                "if header :contains \"Subject\" \"{}\" {{\n  discard;\n  stop;\n}}\n",
                escape_sieve_string(&rule.subject)
            )),
        }
    }
    output
}

fn exactly_one_content<'a>(xml: &'a str, name: &str) -> Result<&'a str> {
    match element_contents(xml, name).as_slice() {
        [value] => Ok(*value),
        _ => bail!("Rule request requires exactly one {name} element."),
    }
}

fn exactly_one_text(xml: &str, name: &str) -> Result<String> {
    let value = xml_text(exactly_one_content(xml, name)?);
    if value.is_empty() {
        bail!("Rule request requires a non-empty {name} value.");
    }
    Ok(value)
}

fn bounded_rule_text(value: &str, field: &str) -> Result<String> {
    if value.is_empty() || value.len() > 128 || value.contains(['\r', '\n']) {
        bail!("{field} must contain 1 through 128 single-line characters.");
    }
    Ok(value.to_string())
}

fn parse_ews_rule_id(value: &str) -> Result<Uuid> {
    let value = value
        .strip_prefix("sieve-rule:")
        .ok_or_else(|| anyhow!("RuleId is not an LPE inbox-rule identifier."))?;
    Uuid::parse_str(value).map_err(|_| anyhow!("RuleId is malformed."))
}

fn ews_rule_id(id: Uuid) -> String {
    format!("sieve-rule:{id}")
}
fn encode_rule_value(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value)
}
fn decode_rule_value(value: &str) -> Option<String> {
    URL_SAFE_NO_PAD
        .decode(value)
        .ok()
        .and_then(|value| String::from_utf8(value).ok())
        .filter(|value| bounded_rule_text(value, "generated rule value").is_ok())
}
fn escape_sieve_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
fn opening_rule_operation_names(xml: &str) -> Vec<String> {
    opening_element_names(xml)
        .into_iter()
        .filter(|name| name.ends_with("RuleOperation"))
        .collect()
}
fn opening_element_names(xml: &str) -> Vec<String> {
    xml.split('<')
        .filter_map(|fragment| {
            let fragment = fragment.trim_start();
            if fragment.starts_with(['/', '!', '?']) {
                return None;
            }
            let name = fragment
                .split(|value: char| value.is_whitespace() || value == '>' || value == '/')
                .next()?;
            (!name.is_empty()).then(|| name.rsplit(':').next().unwrap_or(name).to_string())
        })
        .collect()
}
