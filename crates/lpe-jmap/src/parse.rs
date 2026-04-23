use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use crate::convert::resolve_creation_reference;
use crate::protocol::EmailAddressInput;

pub(crate) fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("invalid id: {value}"))
}

pub(crate) fn parse_uuid_list(value: Option<Vec<String>>) -> Result<Option<Vec<Uuid>>> {
    value
        .map(|values| values.into_iter().map(|value| parse_uuid(&value)).collect())
        .transpose()
}

pub(crate) fn parse_submission_email_id(
    value: &Value,
    created_ids: &HashMap<String, String>,
) -> Result<Option<String>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("submission create arguments must be an object"))?;
    if let Some(email_id) = object.get("emailId").and_then(Value::as_str) {
        return Ok(Some(resolve_creation_reference(email_id, created_ids)));
    }
    if let Some(reference) = object.get("#emailId").and_then(Value::as_str) {
        return Ok(created_ids.get(reference).cloned());
    }
    Ok(None)
}

pub(crate) fn parse_first_property_object_string(
    value: Option<&Value>,
    property_name: &str,
    field_name: &str,
) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} must be an object"))?;
    let Some(first) = object.values().next() else {
        return Ok(String::new());
    };
    let first = first
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} entries must be objects"))?;
    Ok(first
        .get(field_name)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string())
}

pub(crate) fn parse_address_list(value: Option<&Value>) -> Result<Option<Vec<EmailAddressInput>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(Vec::new())),
        Some(value) => Ok(Some(serde_json::from_value(value.clone())?)),
    }
}

pub(crate) fn parse_optional_string(value: Option<&Value>) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(String::new())),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => bail!("string property expected"),
    }
}

pub(crate) fn parse_required_string(value: Option<&Value>, field_name: &str) -> Result<String> {
    value
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{field_name} is required"))
}

pub(crate) fn parse_optional_nullable_string(
    value: Option<&Value>,
) -> Result<Option<Option<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => Ok(Some(Some(value.clone()))),
        _ => bail!("string or null property expected"),
    }
}

pub(crate) fn parse_local_datetime(value: &str) -> Result<(String, String)> {
    let trimmed = value.trim();
    let (date, time) = trimmed
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid local date-time"))?;
    if date.len() != 10 || time.len() < 5 {
        bail!("invalid local date-time");
    }
    let time = time.trim_end_matches('Z');
    let hhmm = if let Some((hours_minutes, _seconds)) = time.split_once(':') {
        if hours_minutes.len() != 2 {
            bail!("invalid local date-time");
        }
        format!("{hours_minutes}:{}", &time[3..5])
    } else {
        bail!("invalid local date-time");
    };
    Ok((date.to_string(), hhmm))
}

pub(crate) fn parse_local_datetime_value(value: &str) -> Result<String> {
    let (date, time) = parse_local_datetime(value)?;
    Ok(format!("{date}T{time}:00"))
}
