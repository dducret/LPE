use super::*;

pub(super) struct BoundedRuleMutation {
    pub(super) name: String,
    pub(super) content: String,
    pub(super) active: bool,
}

pub(super) fn bounded_rule_mutation_from_row(
    row: &ModifyRulesRow,
) -> Result<BoundedRuleMutation, u32> {
    if row
        .properties
        .keys()
        .any(|tag| matches!(*tag, 0x6679_00FD | 0x6680_00FE))
    {
        return Err(EC_RULE_UNSUPPORTED);
    }
    let name = row
        .properties
        .get(&PID_TAG_RULE_NAME_W)
        .and_then(|value| value.clone().into_text())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or(EC_RULE_INVALID_PARAMETER)?;
    let active = row
        .properties
        .get(&PID_TAG_RULE_STATE)
        .and_then(MapiValue::as_i64)
        .map(|value| value as u32 & ST_ENABLED != 0)
        .unwrap_or(true);
    let provider_data = row
        .properties
        .get(&PID_TAG_RULE_PROVIDER_DATA)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => serde_json::from_slice::<Value>(bytes).ok(),
            _ => None,
        })
        .ok_or(EC_RULE_UNSUPPORTED)?;
    let content = bounded_rule_sieve_from_json(&provider_data)?;
    Ok(BoundedRuleMutation {
        name,
        content,
        active,
    })
}

fn bounded_rule_sieve_from_json(value: &Value) -> Result<String, u32> {
    if value
        .get("clientOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || value
            .get("delegate")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || value.get("delegateTemplate").is_some()
        || value
            .get("deferredAction")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || value.get("exchangeBlob").is_some()
    {
        return Err(EC_RULE_UNSUPPORTED);
    }
    let condition = value.get("condition").unwrap_or(&Value::Null);
    let test = match condition
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("always")
    {
        "always" => "true".to_string(),
        "subjectContains" => format!(
            r#"header :contains "Subject" "{}""#,
            sieve_escape(
                condition
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or(EC_RULE_INVALID_PARAMETER)?
            )
        ),
        "fromContains" => format!(
            r#"address :contains "From" "{}""#,
            sieve_escape(
                condition
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or(EC_RULE_INVALID_PARAMETER)?
            )
        ),
        _ => return Err(EC_RULE_UNSUPPORTED),
    };
    let mut requirements = Vec::new();
    let mut actions = Vec::new();
    for action in value
        .get("actions")
        .and_then(Value::as_array)
        .ok_or(EC_RULE_INVALID_PARAMETER)?
    {
        match action.get("type").and_then(Value::as_str) {
            Some("move") => {
                requirements.push("fileinto");
                actions.push(format!(
                    r#"fileinto "{}";"#,
                    sieve_escape(
                        action
                            .get("folder")
                            .and_then(Value::as_str)
                            .ok_or(EC_RULE_INVALID_PARAMETER)?
                    )
                ));
            }
            Some("delete") => actions.push("discard;".to_string()),
            Some("forward") | Some("redirect") => {
                requirements.push("redirect");
                actions.push(format!(
                    r#"redirect "{}";"#,
                    sieve_escape(
                        action
                            .get("address")
                            .and_then(Value::as_str)
                            .ok_or(EC_RULE_INVALID_PARAMETER)?
                    )
                ));
            }
            Some("markRead") => actions.push("keep;".to_string()),
            _ => return Err(EC_RULE_UNSUPPORTED),
        }
    }
    if value
        .get("stopProcessing")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        actions.push("stop;".to_string());
    }
    if actions.is_empty() {
        return Err(EC_RULE_INVALID_PARAMETER);
    }
    requirements.sort_unstable();
    requirements.dedup();
    let require = if requirements.is_empty() {
        String::new()
    } else {
        format!(
            "require [{}];\n",
            requirements
                .iter()
                .map(|requirement| format!(r#""{requirement}""#))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    Ok(format!(
        "{require}if {test} {{\n    {}\n}}",
        actions.join("\n    ")
    ))
}

fn sieve_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub(super) fn rule_audit(
    principal: &AccountPrincipal,
    action: &str,
    subject: &str,
) -> AuditEntryInput {
    AuditEntryInput {
        actor: principal.email.clone(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}
