use anyhow::Result;
use lpe_core::sieve::{Action, Statement};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashSet;

use lpe_storage::AuthenticatedAccount;

use crate::{convert::insert_if, service::opaque_state_fingerprint, JmapService, SESSION_STATE};

const VACATION_RESPONSE_ID: &str = "singleton";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VacationResponseGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
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
