use anyhow::{bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_storage::{JmapEmail, JmapEmailSubmission, JmapQuota, SenderIdentity};
use serde_json::{json, Map, Value};
use std::{cmp::Ordering, collections::HashSet};
use uuid::Uuid;

use crate::{
    convert::{address_value, insert_if},
    protocol::{
        EmailGetArguments, EmailQueryFilter, EmailQuerySort, EmailSubmissionQueryFilter,
        EmailSubmissionQuerySort,
    },
};

pub(crate) fn full_query_limit(total: u64) -> u64 {
    total.max(1).min(i64::MAX as u64)
}

pub(crate) fn serialize_email_query_filter(filter: &EmailQueryFilter) -> Result<Value> {
    Ok(serde_json::to_value(filter)?)
}

pub(crate) fn serialize_email_query_sort(sort: &[EmailQuerySort]) -> Result<Vec<Value>> {
    sort.iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn validate_email_submission_query(
    filter: Option<&EmailSubmissionQueryFilter>,
    sort: Option<&[EmailSubmissionQuerySort]>,
) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(before) = filter.before.as_deref() {
            crate::parse::parse_local_datetime(before)?;
        }
        if let Some(after) = filter.after.as_deref() {
            crate::parse::parse_local_datetime(after)?;
        }
    }
    if let Some(sort) = sort {
        for item in sort {
            if !matches!(
                item.property.as_str(),
                "emailId" | "threadId" | "sentAt" | "sendAt"
            ) {
                bail!("only emailId, threadId, and sentAt sort are supported");
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_email_submission_query(
    submissions: &mut Vec<JmapEmailSubmission>,
    filter: Option<&EmailSubmissionQueryFilter>,
    sort: Option<&[EmailSubmissionQuerySort]>,
) {
    if let Some(filter) = filter {
        submissions.retain(|submission| email_submission_matches_filter(submission, filter));
    }
    if let Some(sort) = sort {
        for item in sort.iter().rev() {
            let ascending = item.is_ascending.unwrap_or(true);
            submissions.sort_by(|left, right| {
                let ordering = compare_email_submission_sort_key(left, right, &item.property);
                if ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            });
        }
    }
}

fn email_submission_matches_filter(
    submission: &JmapEmailSubmission,
    filter: &EmailSubmissionQueryFilter,
) -> bool {
    if let Some(identity_ids) = filter.identity_ids.as_ref() {
        if !identity_ids.contains(&submission.identity_id) {
            return false;
        }
    }
    if let Some(email_ids) = filter.email_ids.as_ref() {
        if !email_ids.contains(&submission.email_id.to_string()) {
            return false;
        }
    }
    if let Some(thread_ids) = filter.thread_ids.as_ref() {
        if !thread_ids.contains(&submission.thread_id.to_string()) {
            return false;
        }
    }
    if let Some(undo_status) = filter.undo_status.as_deref() {
        if submission.undo_status != undo_status {
            return false;
        }
    }
    if let Some(before) = filter.before.as_deref() {
        if submission.send_at.as_str() >= before {
            return false;
        }
    }
    if let Some(after) = filter.after.as_deref() {
        if submission.send_at.as_str() < after {
            return false;
        }
    }
    true
}

fn compare_email_submission_sort_key(
    left: &JmapEmailSubmission,
    right: &JmapEmailSubmission,
    property: &str,
) -> Ordering {
    match property {
        "emailId" => left.email_id.cmp(&right.email_id),
        "threadId" => left.thread_id.cmp(&right.thread_id),
        "sentAt" | "sendAt" => left.send_at.cmp(&right.send_at),
        _ => left.id.cmp(&right.id),
    }
    .then_with(|| left.id.cmp(&right.id))
}

pub(crate) fn serialize_email_submission_query_sort(
    sort: Option<&[EmailSubmissionQuerySort]>,
) -> Result<Option<Vec<Value>>> {
    sort.map(|sort| {
        sort.iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    })
    .transpose()
}

pub(crate) fn email_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "blobId".to_string(),
                "threadId".to_string(),
                "mailboxIds".to_string(),
                "keywords".to_string(),
                "size".to_string(),
                "receivedAt".to_string(),
                "sentAt".to_string(),
                "messageId".to_string(),
                "subject".to_string(),
                "from".to_string(),
                "sender".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "preview".to_string(),
                "hasAttachment".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
                "bodyValues".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn email_submission_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "emailId".to_string(),
                "threadId".to_string(),
                "identityId".to_string(),
                "envelope".to_string(),
                "sendAt".to_string(),
                "undoStatus".to_string(),
                "deliveryStatus".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn identity_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "replyTo".to_string(),
                "bcc".to_string(),
                "textSignature".to_string(),
                "htmlSignature".to_string(),
                "mayDelete".to_string(),
                "xLpeOwnerAccountId".to_string(),
                "xLpeAuthorizationKind".to_string(),
                "xLpeSender".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| vec!["id".to_string(), "emailIds".to_string()])
        .into_iter()
        .collect()
}

pub(crate) struct EmailBodyOptions {
    body_properties: HashSet<String>,
    fetch_text_body_values: bool,
    fetch_html_body_values: bool,
    fetch_all_body_values: bool,
    explicit_fetch_flags: bool,
    max_body_value_bytes: Option<usize>,
}

impl EmailBodyOptions {
    pub(crate) fn from_arguments(arguments: &EmailGetArguments) -> Self {
        let explicit_fetch_flags = arguments.fetch_text_body_values.is_some()
            || arguments.fetch_html_body_values.is_some()
            || arguments.fetch_all_body_values.is_some();
        Self {
            body_properties: arguments
                .body_properties
                .clone()
                .unwrap_or_else(|| {
                    vec![
                        "partId".to_string(),
                        "type".to_string(),
                        "size".to_string(),
                        "charset".to_string(),
                    ]
                })
                .into_iter()
                .collect(),
            fetch_text_body_values: arguments.fetch_text_body_values.unwrap_or(false),
            fetch_html_body_values: arguments.fetch_html_body_values.unwrap_or(false),
            fetch_all_body_values: arguments.fetch_all_body_values.unwrap_or(false),
            explicit_fetch_flags,
            max_body_value_bytes: arguments.max_body_value_bytes.map(|value| value as usize),
        }
    }

    fn should_fetch_text_value(&self) -> bool {
        if self.explicit_fetch_flags {
            self.fetch_all_body_values || self.fetch_text_body_values
        } else {
            true
        }
    }

    fn should_fetch_html_value(&self) -> bool {
        if self.explicit_fetch_flags {
            self.fetch_all_body_values || self.fetch_html_body_values
        } else {
            true
        }
    }
}

pub(crate) fn email_to_value(
    email: &JmapEmail,
    properties: &HashSet<String>,
    body_options: &EmailBodyOptions,
    include_owner_bcc: bool,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", email.id.to_string());
    insert_if(
        properties,
        &mut object,
        "blobId",
        crate::blob_id_for_message(email),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        email.thread_id.to_string(),
    );
    if properties.contains("mailboxIds") {
        let mut mailbox_ids = Map::new();
        for mailbox_id in &email.mailbox_ids {
            mailbox_ids.insert(mailbox_id.to_string(), Value::Bool(true));
        }
        object.insert("mailboxIds".to_string(), Value::Object(mailbox_ids));
    }
    if properties.contains("keywords") {
        object.insert("keywords".to_string(), email_keywords(email));
    }
    if properties.contains("xLpeFollowUp") {
        object.insert("xLpeFollowUp".to_string(), email_followup_value(email));
    }
    insert_if(properties, &mut object, "size", email.size_octets);
    insert_if(
        properties,
        &mut object,
        "receivedAt",
        email.received_at.clone(),
    );
    if let Some(sent_at) = &email.sent_at {
        insert_if(properties, &mut object, "sentAt", sent_at.clone());
    }
    if properties.contains("messageId") {
        object.insert(
            "messageId".to_string(),
            Value::Array(
                email
                    .internet_message_id
                    .as_ref()
                    .map(|message_id| vec![Value::String(message_id.clone())])
                    .unwrap_or_default(),
            ),
        );
    }
    insert_if(properties, &mut object, "subject", email.subject.clone());
    if properties.contains("from") {
        object.insert(
            "from".to_string(),
            Value::Array(vec![address_value(
                &email.from_address,
                email.from_display.as_deref(),
            )]),
        );
    }
    if properties.contains("sender") && email.sender_address.is_some() {
        object.insert(
            "sender".to_string(),
            Value::Array(vec![address_value(
                email.sender_address.as_deref().unwrap_or_default(),
                email.sender_display.as_deref(),
            )]),
        );
    }
    if properties.contains("to") {
        object.insert(
            "to".to_string(),
            Value::Array(
                email
                    .to
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("cc") {
        object.insert(
            "cc".to_string(),
            Value::Array(
                email
                    .cc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if include_owner_bcc && properties.contains("bcc") && !email.bcc.is_empty() {
        object.insert(
            "bcc".to_string(),
            Value::Array(
                email
                    .bcc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    insert_if(properties, &mut object, "preview", email.preview.clone());
    insert_if(
        properties,
        &mut object,
        "hasAttachment",
        email.has_attachments,
    );

    let include_body_values =
        properties.contains("bodyValues") || body_options.explicit_fetch_flags;
    let mut body_values = Map::new();
    if !email.body_text.is_empty() {
        if include_body_values && body_options.should_fetch_text_value() {
            let (value, is_truncated) =
                body_value(&email.body_text, body_options.max_body_value_bytes);
            body_values.insert(
                "textBody".to_string(),
                json!({
                    "value": value,
                    "isEncodingProblem": false,
                    "isTruncated": is_truncated,
                }),
            );
        }
        if properties.contains("textBody") {
            object.insert(
                "textBody".to_string(),
                Value::Array(vec![body_part_value(
                    "textBody",
                    "text/plain",
                    email.body_text.len(),
                    &body_options.body_properties,
                )]),
            );
        }
    }
    if let Some(html) = &email.body_html_sanitized {
        if include_body_values && body_options.should_fetch_html_value() {
            let (value, is_truncated) = body_value(html, body_options.max_body_value_bytes);
            body_values.insert(
                "htmlBody".to_string(),
                json!({
                    "value": value,
                    "isEncodingProblem": false,
                    "isTruncated": is_truncated,
                }),
            );
        }
        if properties.contains("htmlBody") {
            object.insert(
                "htmlBody".to_string(),
                Value::Array(vec![body_part_value(
                    "htmlBody",
                    "text/html",
                    html.len(),
                    &body_options.body_properties,
                )]),
            );
        }
    }
    if include_body_values {
        object.insert("bodyValues".to_string(), Value::Object(body_values));
    }

    Value::Object(object)
}

fn body_part_value(
    part_id: &str,
    content_type: &str,
    size: usize,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "partId", part_id);
    insert_if(properties, &mut object, "type", content_type);
    insert_if(properties, &mut object, "size", size as u64);
    insert_if(properties, &mut object, "charset", "utf-8");
    Value::Object(object)
}

fn body_value(value: &str, max_bytes: Option<usize>) -> (String, bool) {
    let Some(max_bytes) = max_bytes else {
        return (value.to_string(), false);
    };
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut boundary = 0;
    for (index, _) in value.char_indices() {
        if index > max_bytes {
            break;
        }
        boundary = index;
    }
    if boundary == 0 && max_bytes > 0 && value.is_char_boundary(max_bytes) {
        boundary = max_bytes;
    }
    (value[..boundary].to_string(), true)
}

pub(crate) fn email_submission_to_value(
    submission: &JmapEmailSubmission,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", submission.id.to_string());
    insert_if(
        properties,
        &mut object,
        "emailId",
        submission.email_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        submission.thread_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "identityId",
        submission.identity_id.clone(),
    );
    if properties.contains("envelope") {
        object.insert(
            "envelope".to_string(),
            json!({
                "mailFrom": {"email": submission.envelope_mail_from},
                "rcptTo": submission.envelope_rcpt_to.iter().map(|address| json!({"email": address})).collect::<Vec<_>>(),
            }),
        );
    }
    insert_if(
        properties,
        &mut object,
        "sendAt",
        submission.send_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "undoStatus",
        submission.undo_status.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "deliveryStatus",
        submission.delivery_status.clone(),
    );
    Value::Object(object)
}

pub(crate) fn identity_to_value(identity: &SenderIdentity, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", identity.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        identity.display_name.clone(),
    );
    insert_if(properties, &mut object, "email", identity.email.clone());
    if properties.contains("replyTo") {
        object.insert("replyTo".to_string(), Value::Null);
    }
    if properties.contains("bcc") {
        object.insert("bcc".to_string(), Value::Null);
    }
    insert_if(properties, &mut object, "textSignature", "");
    insert_if(properties, &mut object, "htmlSignature", "");
    insert_if(properties, &mut object, "mayDelete", false);
    insert_if(
        properties,
        &mut object,
        "xLpeOwnerAccountId",
        identity.owner_account_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "xLpeAuthorizationKind",
        identity.authorization_kind.clone(),
    );
    if properties.contains("xLpeSender") {
        let sender = identity.sender_address.as_ref().map(|address| {
            json!({
                "email": address,
                "name": identity.sender_display.clone(),
            })
        });
        object.insert("xLpeSender".to_string(), sender.unwrap_or(Value::Null));
    }
    Value::Object(object)
}

pub(crate) fn thread_to_value(
    thread_id: Uuid,
    email_ids: Vec<String>,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", thread_id.to_string());
    if properties.contains("emailIds") {
        object.insert(
            "emailIds".to_string(),
            Value::Array(email_ids.into_iter().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

pub(crate) fn search_snippet_to_value(email: &JmapEmail) -> Value {
    let subject = if email.subject.is_empty() {
        email.preview.clone()
    } else {
        email.subject.clone()
    };
    let preview = if email.preview.is_empty() {
        crate::trim_snippet(&email.body_text, 120)
    } else {
        crate::trim_snippet(&email.preview, 120)
    };
    json!({
        "emailId": email.id.to_string(),
        "subject": subject,
        "preview": preview,
    })
}

pub(crate) fn quota_to_value(quota: &JmapQuota) -> Value {
    json!({
        "id": quota.id,
        "name": quota.name,
        "used": quota.used,
        "hardLimit": quota.hard_limit,
        "scope": "account",
    })
}

pub(crate) fn email_keywords(email: &JmapEmail) -> Value {
    let mut keywords = Map::new();
    if email.mailbox_states.iter().any(|state| state.draft) {
        keywords.insert("$draft".to_string(), Value::Bool(true));
    }
    if !email.unread {
        keywords.insert("$seen".to_string(), Value::Bool(true));
    }
    if email.flagged {
        keywords.insert("$flagged".to_string(), Value::Bool(true));
    }
    for category in &email.categories {
        let category = category.trim();
        if !category.is_empty() {
            keywords.insert(category.to_string(), Value::Bool(true));
        }
    }
    Value::Object(keywords)
}

pub(crate) fn email_followup_value(email: &JmapEmail) -> Value {
    json!({
        "status": email.followup_flag_status,
        "icon": email.followup_icon,
        "todoItemFlags": email.todo_item_flags,
        "request": email.followup_request,
        "startAt": email.followup_start_at,
        "dueAt": email.followup_due_at,
        "completedAt": email.followup_completed_at,
        "reminderSet": email.reminder_set,
        "reminderAt": email.reminder_at,
        "reminderDismissedAt": email.reminder_dismissed_at,
        "swappedToDoStoreId": email.swapped_todo_store_id.map(|id| id.to_string()),
        "swappedToDoData": email.swapped_todo_data.as_ref().map(|data| BASE64.encode(data)),
    })
}
