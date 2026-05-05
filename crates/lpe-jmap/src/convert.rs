use anyhow::{bail, Result};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::protocol::EmailAddressInput;
use lpe_storage::{
    mail::ParsedMailAddress, AuthenticatedAccount, JmapEmailAddress, MailboxAccountAccess,
    SubmittedRecipientInput,
};

pub(crate) fn format_addresses(addresses: &[JmapEmailAddress]) -> String {
    addresses
        .iter()
        .map(|address| {
            format!(
                "{}:{}",
                address.address,
                address.display_name.clone().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn insert_if<T: Serialize>(
    properties: &std::collections::HashSet<String>,
    object: &mut Map<String, Value>,
    key: &str,
    value: T,
) {
    if properties.contains(key) {
        object.insert(
            key.to_string(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        );
    }
}

pub(crate) fn has_jmap_property_patch(object: &Map<String, Value>) -> bool {
    object.keys().any(|key| key.contains('/'))
}

pub(crate) fn apply_jmap_property_patch(
    target: &mut Value,
    patch: &Map<String, Value>,
) -> Result<()> {
    for (key, value) in patch {
        if key.contains('/') {
            apply_jmap_property_path(target, key, value.clone())?;
        } else {
            let object = target
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("patched object must be an object"))?;
            if value.is_null() {
                object.remove(key);
            } else {
                object.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(())
}

fn apply_jmap_property_path(target: &mut Value, path: &str, value: Value) -> Result<()> {
    let segments = path
        .split('/')
        .map(unescape_property_path_segment)
        .collect::<Result<Vec<_>>>()?;
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        bail!("invalid property patch path: {path}");
    }

    let mut current = target;
    for segment in &segments[..segments.len() - 1] {
        let object = current
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("property patch path parent must be an object"))?;
        current = object
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }

    let object = current
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("property patch path parent must be an object"))?;
    let leaf = segments.last().unwrap();
    if value.is_null() {
        object.remove(leaf);
    } else {
        object.insert(leaf.clone(), value);
    }
    Ok(())
}

fn unescape_property_path_segment(segment: &str) -> Result<String> {
    let mut output = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => output.push('~'),
                Some('1') => output.push('/'),
                Some(other) => bail!("invalid property patch escape: ~{other}"),
                None => bail!("invalid trailing property patch escape"),
            }
        } else {
            output.push(ch);
        }
    }
    Ok(output)
}

pub(crate) fn address_value(email: &str, name: Option<&str>) -> Value {
    json!({
        "email": email,
        "name": name,
    })
}

pub(crate) fn resolve_creation_reference(
    value: &str,
    created_ids: &HashMap<String, String>,
) -> String {
    if let Some(reference) = value.strip_prefix('#') {
        created_ids
            .get(reference)
            .cloned()
            .unwrap_or_else(|| value.to_string())
    } else {
        value.to_string()
    }
}

pub(crate) fn select_from_addresses(
    from: Option<Vec<EmailAddressInput>>,
    sender: Option<Vec<EmailAddressInput>>,
    account: &AuthenticatedAccount,
    account_access: &MailboxAccountAccess,
) -> Result<(EmailAddressInput, Option<EmailAddressInput>)> {
    let from = match from {
        None => EmailAddressInput {
            email: account_access.email.clone(),
            name: Some(account_access.display_name.clone()),
        },
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one from address is required");
            }
            let address = addresses.remove(0);
            let normalized = address.email.trim().to_lowercase();
            if normalized != account_access.email {
                bail!("from email must match the selected mailbox account");
            }
            EmailAddressInput {
                email: account_access.email.clone(),
                name: address.name,
            }
        }
    };

    let sender = match sender {
        None => None,
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one sender address is required");
            }
            let address = addresses.remove(0);
            let normalized = address.email.trim().to_lowercase();
            if normalized != account.email {
                bail!("sender email must match authenticated account");
            }
            Some(EmailAddressInput {
                email: account.email.clone(),
                name: address.name.or_else(|| Some(account.display_name.clone())),
            })
        }
    };

    Ok((from, sender))
}

pub(crate) fn map_recipients(
    input: Vec<EmailAddressInput>,
) -> Result<Vec<SubmittedRecipientInput>> {
    input
        .into_iter()
        .map(|recipient| {
            let address = recipient.email.trim().to_lowercase();
            if address.is_empty() {
                bail!("recipient email is required");
            }
            Ok(SubmittedRecipientInput {
                address,
                display_name: recipient.name.and_then(|name| {
                    let trimmed = name.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                }),
            })
        })
        .collect()
}

pub(crate) fn map_existing_recipients(
    recipients: &[JmapEmailAddress],
) -> Vec<SubmittedRecipientInput> {
    recipients
        .iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        })
        .collect()
}

pub(crate) fn map_parsed_recipients(
    recipients: Vec<ParsedMailAddress>,
) -> Vec<SubmittedRecipientInput> {
    recipients
        .into_iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect()
}
