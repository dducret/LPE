---
type: Rust Module
title: submission
resource: crates/lpe-storage/src/submission.rs#L1-L1532
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-mapi-message-identity-rotate-active-mapi-message-identity-in-tx-normalize-email-normalize-subject-sha256-hex-trim-optional-text-auditentryinput-jmapemailrecipientrow-storage
  - external/types-canonical-submission-phases-source-protocol-sql-submission-authorization-kind-sql-canonicalsubmissionphase-resolvedsubmissionauthorization
  - external/pub-crate-use-types-normalize-bcc-recipients-normalize-visible-recipients-participants-normalized-push-recipients-sender-authorization-kind-from-str-sender-identity-id-accountidentity
  - external/pub-use-types-attachmentuploadinput-cancelsubmissionresult-mailboxaccountaccess-mailboxdelegationgrant-mailboxdelegationgrantinput-mailboxdelegationoverview-mailboxfolderdelegationgrantinput-saveddraftmessage-senderauthorizationkind-senderdelegationgrant-senderdelegationgrantinput-senderdelegationright-senderidentity-submissionaccountidentity-submitmessageinput-submittedmessage-submittedrecipientinput
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [insert_visible_recipient](../../../../functions/crates/lpe-storage/src/submission/insert_visible_recipient.md)
- [replace_message_recipients](../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)
- [save_draft_message](../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
- [submit_draft_message](../../../../functions/crates/lpe-storage/src/submission/Storage/submit_draft_message.md)
- [cancel_queued_submission](../../../../functions/crates/lpe-storage/src/submission/Storage/cancel_queued_submission.md)
- [delete_draft_message](../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message.md)
- [account_identity_for_id](../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [load_account_identity_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [load_account_identity_by_email_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx.md)
- [ensure_same_tenant_account_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/ensure_same_tenant_account_in_tx.md)
- [has_sender_right_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/has_sender_right_in_tx.md)
- [resolve_submission_authorization_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/resolve_submission_authorization_in_tx.md)
- [find_submission_account_by_email_in_same_tenant](../../../../functions/crates/lpe-storage/src/submission/Storage/find_submission_account_by_email_in_same_tenant.md)
- [delete_draft_message_in_tx](../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx, normalize_email,
    normalize_subject, sha256_hex, trim_optional_text, AuditEntryInput, JmapEmailRecipientRow,
    Storage,
}`
- `types::{
    canonical_submission_phases, source_protocol_sql, submission_authorization_kind_sql,
    CanonicalSubmissionPhase, ResolvedSubmissionAuthorization,
}`
- `pub(crate) use types::{
    normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    push_recipients, sender_authorization_kind_from_str, sender_identity_id, AccountIdentity,
}`
- `pub use types::{
    AttachmentUploadInput, CancelSubmissionResult, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxDelegationOverview, MailboxFolderDelegationGrantInput,
    SavedDraftMessage, SenderAuthorizationKind, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)