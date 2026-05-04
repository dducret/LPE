use anyhow::{anyhow, bail, Result};
use lpe_core::sieve::{Action, Statement};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashSet;

use lpe_storage::{AuditEntryInput, AuthenticatedAccount};

use crate::{
    convert::{insert_if, resolve_creation_reference},
    error::set_error,
    service::opaque_state_fingerprint,
    JmapService, SESSION_STATE,
};

const VACATION_RESPONSE_ID: &str = "singleton";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VacationResponseGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VacationResponseSetArguments {
    account_id: Option<String>,
    create: Option<Map<String, Value>>,
    update: Option<Map<String, Value>>,
    destroy: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct VacationResponseProjection {
    is_enabled: bool,
    subject: String,
    text_body: String,
}

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_vacation_response_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: VacationResponseGetArguments = serde_json::from_value(arguments)?;
        let account_id = crate::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = vacation_response_properties(arguments.properties);
        let requested_ids = arguments
            .ids
            .unwrap_or_else(|| vec![VACATION_RESPONSE_ID.to_string()]);
        let projection = self.vacation_response_projection(account_id).await?;
        let state = vacation_response_state(&projection);

        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in requested_ids {
            if id == VACATION_RESPONSE_ID {
                list.push(vacation_response_to_value(&projection, &properties));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_vacation_response_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut std::collections::HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: VacationResponseSetArguments = serde_json::from_value(arguments)?;
        let account_id = crate::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_projection = self.vacation_response_projection(account_id).await?;
        let old_state = vacation_response_state(&old_projection);
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (client_id, value) in create {
                if old_projection.is_enabled {
                    not_created.insert(
                        client_id,
                        set_error("VacationResponse already exists; update singleton instead"),
                    );
                    continue;
                }
                let disabled = VacationResponseProjection::disabled();
                match save_vacation_response(self, account_id, &value, &disabled, account).await {
                    Ok(projection) => {
                        created_ids.insert(client_id.clone(), VACATION_RESPONSE_ID.to_string());
                        created.insert(
                            client_id,
                            vacation_response_to_value(
                                &projection,
                                &vacation_response_properties(None),
                            ),
                        );
                    }
                    Err(error) => {
                        not_created.insert(client_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, patch) in update {
                let id = resolve_creation_reference(&id, created_ids);
                if id != VACATION_RESPONSE_ID {
                    not_updated.insert(id, set_error("VacationResponse not found"));
                    continue;
                }
                match save_vacation_response(self, account_id, &patch, &old_projection, account)
                    .await
                {
                    Ok(projection) => {
                        updated.insert(
                            id,
                            vacation_response_to_value(
                                &projection,
                                &vacation_response_properties(None),
                            ),
                        );
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(destroy) = arguments.destroy {
            for id in destroy {
                let id = resolve_creation_reference(&id, created_ids);
                if id != VACATION_RESPONSE_ID {
                    not_destroyed.insert(id, set_error("VacationResponse not found"));
                    continue;
                }
                match self
                    .store
                    .set_active_sieve_script(
                        account_id,
                        None,
                        vacation_audit(account, account_id, "jmap-vacation-disable"),
                    )
                    .await
                {
                    Ok(_) => destroyed.push(id),
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_projection = self.vacation_response_projection(account_id).await?;
        let new_state = vacation_response_state(&new_projection);

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn vacation_response_projection(
        &self,
        account_id: uuid::Uuid,
    ) -> Result<VacationResponseProjection> {
        let Some(script) = self.store.fetch_active_sieve_script(account_id).await? else {
            return Ok(VacationResponseProjection::disabled());
        };
        let Ok(script) = lpe_core::sieve::parse_script(&script.content) else {
            return Ok(VacationResponseProjection::disabled());
        };
        let Some(action) = find_vacation_action(&script.statements) else {
            return Ok(VacationResponseProjection::disabled());
        };
        Ok(VacationResponseProjection {
            is_enabled: true,
            subject: action.0.unwrap_or_default(),
            text_body: action.1,
        })
    }
}

async fn save_vacation_response<S: crate::store::JmapStore, V: lpe_magika::Detector>(
    service: &JmapService<S, V>,
    account_id: uuid::Uuid,
    value: &Value,
    existing: &VacationResponseProjection,
    account: &AuthenticatedAccount,
) -> Result<VacationResponseProjection> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("VacationResponse/set value must be an object"))?;
    let is_enabled = object
        .get("isEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(existing.is_enabled);
    if !is_enabled {
        service
            .store
            .set_active_sieve_script(
                account_id,
                None,
                vacation_audit(account, account_id, "jmap-vacation-disable"),
            )
            .await?;
        return Ok(VacationResponseProjection::disabled());
    }

    if object.get("fromDate").is_some_and(|value| !value.is_null())
        || object.get("toDate").is_some_and(|value| !value.is_null())
        || object.get("htmlBody").is_some_and(|value| !value.is_null())
    {
        bail!("fromDate, toDate, and htmlBody are not supported by the canonical Sieve MVP");
    }
    let subject = object
        .get("subject")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| existing.subject.clone());
    let text_body = object
        .get("textBody")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| existing.text_body.clone());
    if text_body.is_empty() {
        bail!("textBody is required when enabling VacationResponse");
    }

    let content = vacation_sieve_script(&subject, &text_body);
    service
        .store
        .put_sieve_script(
            account_id,
            "jmap-vacation",
            &content,
            true,
            vacation_audit(account, account_id, "jmap-vacation-enable"),
        )
        .await?;
    Ok(VacationResponseProjection {
        is_enabled: true,
        subject,
        text_body,
    })
}

fn vacation_sieve_script(subject: &str, text_body: &str) -> String {
    let subject = sieve_quote(subject);
    let text_body = sieve_quote(text_body);
    if subject.is_empty() {
        format!("require [\"vacation\"];\r\nvacation :days 7 \"{text_body}\";\r\n")
    } else {
        format!(
            "require [\"vacation\"];\r\nvacation :subject \"{subject}\" :days 7 \"{text_body}\";\r\n"
        )
    }
}

fn sieve_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn vacation_audit(
    account: &AuthenticatedAccount,
    subject_account_id: uuid::Uuid,
    action: &str,
) -> AuditEntryInput {
    AuditEntryInput {
        actor: account.email.clone(),
        action: action.to_string(),
        subject: subject_account_id.to_string(),
    }
}

impl VacationResponseProjection {
    fn disabled() -> Self {
        Self {
            is_enabled: false,
            subject: String::new(),
            text_body: String::new(),
        }
    }
}

fn find_vacation_action(statements: &[Statement]) -> Option<(Option<String>, String)> {
    for statement in statements {
        match statement {
            Statement::Action(Action::Vacation {
                subject, reason, ..
            }) => {
                return Some((subject.clone(), reason.clone()));
            }
            Statement::If {
                branches,
                else_block,
            } => {
                for (_, branch) in branches {
                    if let Some(action) = find_vacation_action(branch) {
                        return Some(action);
                    }
                }
                if let Some(else_block) = else_block {
                    if let Some(action) = find_vacation_action(else_block) {
                        return Some(action);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn vacation_response_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "isEnabled".to_string(),
                "fromDate".to_string(),
                "toDate".to_string(),
                "subject".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn vacation_response_to_value(
    projection: &VacationResponseProjection,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", VACATION_RESPONSE_ID);
    insert_if(properties, &mut object, "isEnabled", projection.is_enabled);
    if properties.contains("fromDate") {
        object.insert("fromDate".to_string(), Value::Null);
    }
    if properties.contains("toDate") {
        object.insert("toDate".to_string(), Value::Null);
    }
    insert_if(
        properties,
        &mut object,
        "subject",
        projection.subject.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "textBody",
        projection.text_body.clone(),
    );
    if properties.contains("htmlBody") {
        object.insert("htmlBody".to_string(), Value::Null);
    }
    Value::Object(object)
}

fn vacation_response_state(projection: &VacationResponseProjection) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}",
        SESSION_STATE, projection.is_enabled, projection.subject, projection.text_body
    ))
}
