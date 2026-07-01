use anyhow::Result;
use lpe_storage::{CollaborationResourceKind, SenderDelegationRight};
use serde_json::{json, Map, Value};
use uuid::Uuid;
pub(super) fn project_share(share_type: &str, value: Value) -> Result<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("share projection must be an object"))?;
    let grant_id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("share projection is missing id"))?
        .to_string();
    let mut projected = Map::new();
    projected.insert(
        "id".to_string(),
        Value::String(format!("{share_type}:{grant_id}")),
    );
    projected.insert("@type".to_string(), Value::String("Share".to_string()));
    projected.insert("type".to_string(), Value::String(share_type.to_string()));
    projected.insert("grantId".to_string(), Value::String(grant_id));
    copy_share_field(object, &mut projected, "ownerAccountId");
    copy_share_field(object, &mut projected, "ownerEmail");
    copy_share_field(object, &mut projected, "ownerDisplayName");
    copy_share_field(object, &mut projected, "granteeAccountId");
    copy_share_field(object, &mut projected, "granteeEmail");
    copy_share_field(object, &mut projected, "granteeDisplayName");
    copy_share_field_as(object, &mut projected, "createdAt", "created");
    copy_share_field_as(object, &mut projected, "updatedAt", "updated");
    match share_type {
        "mailbox" => {
            projected.insert(
                "rights".to_string(),
                json!({
                    "mayRead": true,
                    "mayWrite": object.get("mayWrite").and_then(Value::as_bool).unwrap_or(false),
                    "mayDelete": false,
                    "mayShare": false,
                    "maySend": false
                }),
            );
        }
        "sender" => {
            let sender_right = object
                .get("senderRight")
                .and_then(Value::as_str)
                .unwrap_or("send_on_behalf");
            projected.insert(
                "senderRight".to_string(),
                Value::String(sender_right.to_string()),
            );
            projected.insert(
                "rights".to_string(),
                json!({
                    "mayRead": false,
                    "mayWrite": false,
                    "mayDelete": false,
                    "mayShare": false,
                    "maySend": true,
                    "maySendAs": sender_right == "send_as",
                    "maySendOnBehalf": sender_right == "send_on_behalf"
                }),
            );
        }
        "contacts" | "calendar" | "tasks" => {
            if share_type == "calendar" {
                copy_share_field(object, &mut projected, "calendarId");
                copy_share_field(object, &mut projected, "calendarName");
            }
            projected.insert(
                "rights".to_string(),
                share_rights(object).unwrap_or_else(default_share_rights),
            );
        }
        "taskList" => {
            copy_share_field(object, &mut projected, "taskListId");
            copy_share_field(object, &mut projected, "taskListName");
            projected.insert(
                "rights".to_string(),
                share_rights(object).unwrap_or_else(default_share_rights),
            );
        }
        _ => anyhow::bail!("unsupported share type"),
    }
    Ok(Value::Object(projected))
}

fn copy_share_field(source: &Map<String, Value>, target: &mut Map<String, Value>, field: &str) {
    copy_share_field_as(source, target, field, field);
}

fn copy_share_field_as(
    source: &Map<String, Value>,
    target: &mut Map<String, Value>,
    source_field: &str,
    target_field: &str,
) {
    if let Some(value) = source.get(source_field).filter(|value| !value.is_null()) {
        target.insert(target_field.to_string(), value.clone());
    }
}

fn share_rights(object: &Map<String, Value>) -> Option<Value> {
    object.get("rights").cloned().or_else(|| {
        Some(json!({
            "mayRead": object.get("mayRead")?.as_bool()?,
            "mayWrite": object.get("mayWrite").and_then(Value::as_bool).unwrap_or(false),
            "mayDelete": object.get("mayDelete").and_then(Value::as_bool).unwrap_or(false),
            "mayShare": object.get("mayShare").and_then(Value::as_bool).unwrap_or(false)
        }))
    })
}

fn default_share_rights() -> Value {
    json!({
        "mayRead": true,
        "mayWrite": false,
        "mayDelete": false,
        "mayShare": false
    })
}

pub(super) fn share_type(share: &Value) -> Result<&str> {
    share
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("share type is required"))
}

pub(super) fn share_uuid(share: &Value, field: &str) -> Result<Uuid> {
    share
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{field} is required"))?
        .parse()
        .map_err(Into::into)
}

pub(super) fn parse_collaboration_kind(value: &str) -> Result<CollaborationResourceKind> {
    match value {
        "contacts" => Ok(CollaborationResourceKind::Contacts),
        "calendar" => Ok(CollaborationResourceKind::Calendar),
        "tasks" => Ok(CollaborationResourceKind::Tasks),
        _ => anyhow::bail!("unsupported collaboration share type"),
    }
}

pub(super) fn parse_sender_right(value: Option<&str>) -> Result<SenderDelegationRight> {
    match value.unwrap_or("send_on_behalf") {
        "send_as" => Ok(SenderDelegationRight::SendAs),
        "send_on_behalf" => Ok(SenderDelegationRight::SendOnBehalf),
        _ => anyhow::bail!("unsupported sender right"),
    }
}
