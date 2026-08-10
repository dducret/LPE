use anyhow::{anyhow, bail, Result};
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    parse::{
        parse_address_list, parse_optional_nullable_string, parse_optional_string, parse_uuid,
    },
    protocol::DraftMutation,
    resolve_creation_reference,
};

pub(crate) fn parse_draft_mutation(value: Value) -> Result<DraftMutation> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("email arguments must be an object"))?;
    reject_unknown_email_properties(object)?;

    if let Some(mailbox_ids) = object.get("mailboxIds").and_then(Value::as_object) {
        if mailbox_ids.len() > 1 {
            bail!("only one mailboxId is supported");
        }
    }
    let keywords = parse_draft_keywords(object.get("keywords"))?;

    Ok(DraftMutation {
        from: parse_address_list(object.get("from"))?,
        sender: parse_address_list(object.get("sender"))?,
        to: parse_address_list(object.get("to"))?,
        cc: parse_address_list(object.get("cc"))?,
        bcc: parse_address_list(object.get("bcc"))?,
        subject: parse_optional_string(object.get("subject"))?,
        text_body: parse_optional_string(object.get("textBody"))?,
        html_body: parse_optional_nullable_string(object.get("htmlBody"))?,
        unread: keywords.unread,
        flagged: keywords.flagged,
        attachments: object
            .get("attachments")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?,
    })
}

pub(crate) enum OrdinaryMailboxMutation {
    Replace(Vec<String>),
    Patch(HashMap<String, bool>),
}

pub(crate) fn parse_ordinary_email_mutation(
    value: Value,
) -> Result<(Option<bool>, Option<bool>, Option<OrdinaryMailboxMutation>)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("email arguments must be an object"))?;
    let mut unread = None;
    let mut flagged = None;
    let mut mailbox_ids = None;
    let mut mailbox_patches = HashMap::new();
    for key in object.keys() {
        match key.as_str() {
            "keywords" => {
                if unread.is_some() || flagged.is_some() {
                    bail!("keywords may not be combined with keyword property patches");
                }
                let keywords = parse_ordinary_keywords(object.get(key))?;
                unread = keywords.unread;
                flagged = keywords.flagged;
            }
            "mailboxIds" => {
                if !mailbox_patches.is_empty() {
                    bail!("mailboxIds may not be combined with mailboxIds property patches");
                }
                mailbox_ids = Some(OrdinaryMailboxMutation::Replace(
                    object
                        .get(key)
                        .and_then(Value::as_object)
                        .ok_or_else(|| anyhow!("mailboxIds must be an object"))?
                        .iter()
                        .filter_map(|(id, present)| {
                            present
                                .as_bool()
                                .filter(|present| *present)
                                .map(|_| id.clone())
                        })
                        .collect(),
                ));
            }
            key if key.starts_with("keywords/") => {
                if object.contains_key("keywords") {
                    bail!("keywords may not be combined with keyword property patches");
                }
                let enabled = match object.get(key) {
                    Some(Value::Bool(enabled)) => *enabled,
                    Some(Value::Null) => false,
                    _ => bail!("keyword property patch must be a boolean or null"),
                };
                match &key["keywords/".len()..] {
                    "$seen" => unread = Some(!enabled),
                    "$flagged" => flagged = Some(enabled),
                    _ => bail!("delivered email content is immutable"),
                }
            }
            key if key.starts_with("mailboxIds/") => {
                if object.contains_key("mailboxIds") {
                    bail!("mailboxIds may not be combined with mailboxIds property patches");
                }
                let mailbox_id = &key["mailboxIds/".len()..];
                if mailbox_id.is_empty() {
                    bail!("mailboxIds property patch requires a mailbox id");
                }
                let present = match object.get(key) {
                    Some(Value::Bool(present)) => *present,
                    Some(Value::Null) => false,
                    _ => bail!("mailboxIds property patch must be a boolean or null"),
                };
                mailbox_patches.insert(mailbox_id.to_string(), present);
            }
            _ => bail!("delivered email content is immutable"),
        }
    }
    if !mailbox_patches.is_empty() {
        mailbox_ids = Some(OrdinaryMailboxMutation::Patch(mailbox_patches));
    }
    Ok((unread, flagged, mailbox_ids))
}

#[derive(Default)]
struct ParsedDraftKeywords {
    unread: Option<bool>,
    flagged: Option<bool>,
}

fn parse_draft_keywords(value: Option<&Value>) -> Result<ParsedDraftKeywords> {
    let Some(keywords) = value.and_then(Value::as_object) else {
        return Ok(ParsedDraftKeywords::default());
    };

    let mut parsed = ParsedDraftKeywords::default();
    for (keyword, enabled) in keywords {
        let enabled = enabled
            .as_bool()
            .ok_or_else(|| anyhow!("keyword {keyword} must be a boolean"))?;
        match keyword.as_str() {
            "$draft" => {
                if !enabled {
                    bail!("Email/set is limited to draft messages");
                }
            }
            "$seen" => parsed.unread = Some(!enabled),
            "$flagged" => parsed.flagged = Some(enabled),
            _ => bail!("unsupported keyword: {keyword}"),
        }
    }

    Ok(parsed)
}

fn parse_ordinary_keywords(value: Option<&Value>) -> Result<ParsedDraftKeywords> {
    let Some(keywords) = value.and_then(Value::as_object) else {
        return Ok(ParsedDraftKeywords::default());
    };

    let mut parsed = ParsedDraftKeywords::default();
    for (keyword, enabled) in keywords {
        let enabled = enabled
            .as_bool()
            .ok_or_else(|| anyhow!("keyword {keyword} must be a boolean"))?;
        match keyword.as_str() {
            "$seen" => parsed.unread = Some(!enabled),
            "$flagged" => parsed.flagged = Some(enabled),
            _ => bail!("delivered email content is immutable"),
        }
    }
    Ok(parsed)
}

pub(crate) fn parse_email_copy(
    value: Value,
    created_ids: &HashMap<String, String>,
) -> Result<(Uuid, Uuid)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("Email/copy create arguments must be an object"))?;
    let email_id = object
        .get("emailId")
        .and_then(Value::as_str)
        .map(|value| resolve_creation_reference(value, created_ids))
        .ok_or_else(|| anyhow!("emailId is required"))?;
    let mailbox_ids = object
        .get("mailboxIds")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("mailboxIds is required"))?;
    let mailbox_id = mailbox_ids
        .iter()
        .find(|(_, value)| value.as_bool().unwrap_or(false))
        .map(|(id, _)| parse_uuid(id))
        .transpose()?
        .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
    Ok((parse_uuid(&email_id)?, mailbox_id))
}

pub(crate) fn reject_unknown_email_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "from" | "sender" | "to" | "cc" | "bcc" | "subject" | "textBody" | "htmlBody"
            | "mailboxIds" | "keywords" | "attachments" => {}
            _ => bail!("unsupported email property: {key}"),
        }
    }
    Ok(())
}
