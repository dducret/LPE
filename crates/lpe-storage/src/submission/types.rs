use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{normalize_email, MailboxDelegationGrantRow, SenderDelegationGrantRow};

#[derive(Debug, Clone)]
pub struct SubmitMessageInput {
    pub draft_message_id: Option<Uuid>,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub source: String,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub size_octets: i64,
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
    pub attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, Clone)]
pub struct SubmittedRecipientInput {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentUploadInput {
    pub file_name: String,
    pub media_type: String,
    pub disposition: Option<String>,
    pub content_id: Option<String>,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SubmissionAccountIdentity {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmittedMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub sent_mailbox_id: Uuid,
    pub outbound_queue_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelSubmissionResult {
    Cancelled,
    AlreadyCancelled,
    NotCancellable,
    NotFound,
}

#[derive(Debug, Clone, Serialize)]
pub struct SavedDraftMessage {
    pub message_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub draft_mailbox_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CanonicalSubmissionPhase {
    EnsureSentMailbox,
    PersistSentMessage,
    PersistOutboundQueue,
    DeleteSourceDraft,
}

pub(super) fn canonical_submission_phases(has_source_draft: bool) -> Vec<CanonicalSubmissionPhase> {
    let mut phases = vec![
        CanonicalSubmissionPhase::EnsureSentMailbox,
        CanonicalSubmissionPhase::PersistSentMessage,
        CanonicalSubmissionPhase::PersistOutboundQueue,
    ];
    if has_source_draft {
        phases.push(CanonicalSubmissionPhase::DeleteSourceDraft);
    }
    phases
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SenderAuthorizationKind {
    SelfSend,
    SendAs,
    SendOnBehalf,
}

impl SenderAuthorizationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SelfSend => "self",
            Self::SendAs => "send-as",
            Self::SendOnBehalf => "send-on-behalf",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SenderDelegationRight {
    SendAs,
    SendOnBehalf,
}

impl SenderDelegationRight {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SendAs => "send_as",
            Self::SendOnBehalf => "send_on_behalf",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxAccountAccess {
    pub tenant_id: Uuid,
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub is_owned: bool,
    pub may_read: bool,
    pub may_write: bool,
    pub may_send_as: bool,
    pub may_send_on_behalf: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderIdentity {
    pub id: String,
    pub owner_account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub authorization_kind: String,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MailboxDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub may_write: bool,
}

#[derive(Debug, Clone)]
pub struct MailboxFolderDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub mailbox_id: Uuid,
    pub grantee_account_id: Uuid,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone)]
pub struct SenderDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub sender_right: SenderDelegationRight,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub may_write: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub sender_right: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationOverview {
    pub outgoing_mailboxes: Vec<MailboxDelegationGrant>,
    pub incoming_mailboxes: Vec<MailboxAccountAccess>,
    pub outgoing_sender_rights: Vec<SenderDelegationGrant>,
}

#[derive(Debug, Clone)]
pub(crate) struct AccountIdentity {
    pub(crate) id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
}

#[derive(Debug)]
pub(super) struct ResolvedSubmissionAuthorization {
    pub(super) submitted_by: AccountIdentity,
    pub(super) from_address: String,
    pub(super) from_display: Option<String>,
    pub(super) sender_address: Option<String>,
    pub(super) sender_display: Option<String>,
    pub(super) authorization_kind: SenderAuthorizationKind,
}

pub(super) fn map_mailbox_delegation_grant(
    row: MailboxDelegationGrantRow,
) -> MailboxDelegationGrant {
    MailboxDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        may_write: row.may_write,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(super) fn map_sender_delegation_grant(row: SenderDelegationGrantRow) -> SenderDelegationGrant {
    SenderDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        sender_right: row.sender_right,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(super) fn validate_mailbox_delegation_rights(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> Result<()> {
    if !may_read && (may_write || may_delete || may_share) {
        bail!("read access is required when granting write, delete, or share");
    }
    if may_delete && !may_write {
        bail!("delete access requires write access");
    }
    if may_share && !may_write {
        bail!("share access requires write access");
    }
    Ok(())
}

pub(crate) fn normalize_visible_recipients(
    input: &SubmitMessageInput,
) -> Vec<(&'static str, SubmittedRecipientInput)> {
    let mut recipients = Vec::new();
    push_recipients(&mut recipients, "to", &input.to);
    push_recipients(&mut recipients, "cc", &input.cc);
    recipients
}

pub(crate) fn normalize_bcc_recipients(input: &SubmitMessageInput) -> Vec<SubmittedRecipientInput> {
    let mut recipients = Vec::new();
    push_bcc_recipients(&mut recipients, &input.bcc);
    recipients
}

pub(crate) fn push_recipients(
    output: &mut Vec<(&'static str, SubmittedRecipientInput)>,
    kind: &'static str,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push((
            kind,
            SubmittedRecipientInput {
                address,
                display_name: recipient
                    .display_name
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
        ));
    }
}

fn push_bcc_recipients(
    output: &mut Vec<SubmittedRecipientInput>,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push(SubmittedRecipientInput {
            address,
            display_name: recipient
                .display_name
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        });
    }
}

pub(crate) fn participants_normalized(
    from_address: &str,
    recipients: &[(&'static str, SubmittedRecipientInput)],
) -> String {
    let mut participants = Vec::with_capacity(recipients.len() + 1);
    if !from_address.is_empty() {
        participants.push(from_address.to_string());
    }
    participants.extend(
        recipients
            .iter()
            .map(|(_, recipient)| recipient.address.clone()),
    );
    participants.join(" ")
}

pub(crate) fn sender_authorization_kind_from_str(value: &str) -> SenderAuthorizationKind {
    match value.trim() {
        "send-as" => SenderAuthorizationKind::SendAs,
        "send-on-behalf" => SenderAuthorizationKind::SendOnBehalf,
        _ => SenderAuthorizationKind::SelfSend,
    }
}

pub(crate) fn sender_identity_id(kind: SenderAuthorizationKind, owner_account_id: Uuid) -> String {
    format!("{}:{}", kind.as_str(), owner_account_id)
}

pub(super) fn submission_authorization_kind_sql(kind: SenderAuthorizationKind) -> &'static str {
    match kind {
        SenderAuthorizationKind::SelfSend => "self",
        SenderAuthorizationKind::SendAs => "send_as",
        SenderAuthorizationKind::SendOnBehalf => "send_on_behalf",
    }
}

pub(super) fn source_protocol_sql(source: &str) -> &'static str {
    match source.trim().to_lowercase().as_str() {
        "web" => "web",
        "jmap" => "jmap",
        "ews" => "ews",
        "mapi" => "mapi",
        "activesync" => "activesync",
        "lpe_ct_submission" | "lpe-ct-submission" => "lpe_ct_submission",
        _ => "jmap",
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_submission_phases, CanonicalSubmissionPhase};

    #[test]
    fn canonical_submission_persists_sent_before_queue_handoff() {
        assert_eq!(
            canonical_submission_phases(false),
            vec![
                CanonicalSubmissionPhase::EnsureSentMailbox,
                CanonicalSubmissionPhase::PersistSentMessage,
                CanonicalSubmissionPhase::PersistOutboundQueue,
            ]
        );
    }

    #[test]
    fn draft_submission_deletes_source_only_after_queue_persistence() {
        assert_eq!(
            canonical_submission_phases(true),
            vec![
                CanonicalSubmissionPhase::EnsureSentMailbox,
                CanonicalSubmissionPhase::PersistSentMessage,
                CanonicalSubmissionPhase::PersistOutboundQueue,
                CanonicalSubmissionPhase::DeleteSourceDraft,
            ]
        );
    }
}
