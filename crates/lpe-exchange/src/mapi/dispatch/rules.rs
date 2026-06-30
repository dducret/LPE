use super::*;

pub(super) fn append_get_rules_table_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_handle_index_error_response(request));
        return;
    };
    if folder_row_for_id(folder_id, mailboxes).is_none()
        && role_for_folder_id(folder_id).is_none()
        && snapshot.public_folder_for_id(folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x3F,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    }
    let handle =
        session.allocate_output_handle(request.output_handle_index, rule_table_object(folder_id));
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&get_rules_table_response(request));
    output_handles.push(handle);
}

pub(super) struct BoundedRuleMutation {
    pub(super) name: String,
    pub(super) content: String,
    pub(super) active: bool,
}

pub(super) async fn append_modify_rules_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_handle_index_error_response(request));
        return;
    };
    if folder_row_for_id(folder_id, mailboxes).is_none() && role_for_folder_id(folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x41,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    }
    let rows = match request.modify_rules_rows() {
        Ok(rows) => rows,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x41,
                request.response_handle_index(),
                EC_RULE_INVALID_PARAMETER,
            ));
            return;
        }
    };
    let mut failed = None;
    for row in rows {
        let row_kind = row.flags & (ROW_ADD | ROW_MODIFY | ROW_REMOVE);
        if row_kind == ROW_REMOVE {
            let Some(rule_id) = row
                .properties
                .get(&PID_TAG_RULE_ID)
                .and_then(MapiValue::as_i64)
                .map(|value| value.max(0) as u64)
            else {
                failed = Some(EC_RULE_INVALID_PARAMETER);
                break;
            };
            let Some(rule) = snapshot.rules().iter().find(|rule| rule.id == rule_id) else {
                failed = Some(EC_RULE_NOT_FOUND);
                break;
            };
            if store
                .delete_sieve_script(
                    principal.account_id,
                    &rule.name,
                    rule_audit(principal, "mapi.rule.delete", &rule.name),
                )
                .await
                .is_err()
            {
                failed = Some(EC_RULE_NOT_FOUND);
                break;
            }
            continue;
        }
        if row_kind != ROW_ADD && row_kind != ROW_MODIFY {
            failed = Some(EC_RULE_UNSUPPORTED);
            break;
        }
        let mutation = match bounded_rule_mutation_from_row(&row) {
            Ok(mutation) => mutation,
            Err(error) => {
                failed = Some(error);
                break;
            }
        };
        if store
            .put_sieve_script(
                principal.account_id,
                &mutation.name,
                &mutation.content,
                mutation.active,
                rule_audit(principal, "mapi.rule.upsert", &mutation.name),
            )
            .await
            .is_err()
        {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        }
    }
    if let Some(error) = failed {
        responses.extend_from_slice(&rop_error_response(
            0x41,
            request.response_handle_index(),
            error,
        ));
    } else {
        responses.extend_from_slice(&rop_simple_success_response(request));
    }
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
