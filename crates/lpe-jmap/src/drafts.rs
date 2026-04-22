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
    })
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
            | "mailboxIds" | "keywords" => {}
            _ => bail!("unsupported email property: {key}"),
        }
    }
    Ok(())
}
