use lpe_storage::{JmapEmail, JmapEmailAddress, JmapMailbox};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JmapMailboxObject {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub role: Option<String>,
    pub sort_order: i32,
    pub total_emails: u32,
    pub unread_emails: u32,
    pub my_rights: JmapMailboxRights,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JmapMailboxRights {
    pub may_read_items: bool,
    pub may_add_items: bool,
    pub may_remove_items: bool,
    pub may_create_child: bool,
    pub may_rename: bool,
    pub may_delete: bool,
    pub may_submit: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JmapEmailObject {
    pub id: String,
    pub blob_id: String,
    pub thread_id: String,
    pub mailbox_ids: HashMap<String, bool>,
    pub keywords: HashMap<String, bool>,
    pub size: i64,
    pub received_at: String,
    pub message_id: Vec<String>,
    pub from: Vec<JmapAddressObject>,
    pub sender: Option<Vec<JmapAddressObject>>,
    pub to: Vec<JmapAddressObject>,
    pub cc: Vec<JmapAddressObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bcc: Option<Vec<JmapAddressObject>>,
    pub subject: String,
    pub preview: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JmapThreadObject {
    pub id: String,
    pub email_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JmapAddressObject {
    pub email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl JmapMailboxObject {
    pub fn from_canonical(mailbox: &JmapMailbox, rights: JmapMailboxRights) -> Self {
        Self {
            id: mailbox.id.to_string(),
            name: mailbox.name.clone(),
            parent_id: None,
            role: if mailbox.role.is_empty() {
                None
            } else {
                Some(mailbox.role.clone())
            },
            sort_order: mailbox.sort_order,
            total_emails: mailbox.total_emails,
            unread_emails: mailbox.unread_emails,
            my_rights: rights,
        }
    }
}

impl JmapEmailObject {
    pub fn from_canonical(email: &JmapEmail, include_bcc: bool) -> Self {
        let mut mailbox_ids = HashMap::new();
        mailbox_ids.insert(email.mailbox_id.to_string(), true);

        let mut keywords = HashMap::new();
        if !email.unread {
            keywords.insert("$seen".to_string(), true);
        }
        if email.flagged {
            keywords.insert("$flagged".to_string(), true);
        }
        if email.mailbox_role == "drafts" {
            keywords.insert("$draft".to_string(), true);
        }

        Self {
            id: email.id.to_string(),
            blob_id: email
                .mime_blob_ref
                .clone()
                .unwrap_or_else(|| format!("message:{}", email.id)),
            thread_id: email.thread_id.to_string(),
            mailbox_ids,
            keywords,
            size: email.size_octets,
            received_at: email.received_at.clone(),
            message_id: email
                .internet_message_id
                .iter()
                .map(|value| value.trim_matches(['<', '>']).to_string())
                .collect(),
            from: vec![JmapAddressObject {
                email: email.from_address.clone(),
                name: email.from_display.clone(),
            }],
            sender: email.sender_address.as_ref().map(|address| {
                vec![JmapAddressObject {
                    email: address.clone(),
                    name: email.sender_display.clone(),
                }]
            }),
            to: jmap_addresses(&email.to),
            cc: jmap_addresses(&email.cc),
            bcc: include_bcc.then(|| jmap_addresses(&email.bcc)),
            subject: email.subject.clone(),
            preview: email.preview.clone(),
        }
    }
}

impl JmapThreadObject {
    pub fn from_email_ids(
        id: impl Into<String>,
        email_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            id: id.into(),
            email_ids: email_ids.into_iter().collect(),
        }
    }
}

fn jmap_addresses(addresses: &[JmapEmailAddress]) -> Vec<JmapAddressObject> {
    addresses
        .iter()
        .map(|address| JmapAddressObject {
            email: address.address.clone(),
            name: address.display_name.clone(),
        })
        .collect()
}
