---
type: Rust Module
title: types
resource: crates/lpe-storage/src/submission/types.rs#L1-L428
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/uuid-uuid
  - external/crate-normalize-email-mailboxdelegationgrantrow-senderdelegationgrantrow
  - external/super-canonical-submission-phases-canonicalsubmissionphase
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [SubmitMessageInput](../../../../../classes/crates/lpe-storage/src/submission/types/SubmitMessageInput.md)
- [SubmittedRecipientInput](../../../../../classes/crates/lpe-storage/src/submission/types/SubmittedRecipientInput.md)
- [AttachmentUploadInput](../../../../../classes/crates/lpe-storage/src/submission/types/AttachmentUploadInput.md)
- [SubmissionAccountIdentity](../../../../../classes/crates/lpe-storage/src/submission/types/SubmissionAccountIdentity.md)
- [SubmittedMessage](../../../../../classes/crates/lpe-storage/src/submission/types/SubmittedMessage.md)
- [CancelSubmissionResult](../../../../../classes/crates/lpe-storage/src/submission/types/CancelSubmissionResult.md)
- [SavedDraftMessage](../../../../../classes/crates/lpe-storage/src/submission/types/SavedDraftMessage.md)
- [CanonicalSubmissionPhase](../../../../../classes/crates/lpe-storage/src/submission/types/CanonicalSubmissionPhase.md)
- [canonical_submission_phases](../../../../../functions/crates/lpe-storage/src/submission/types/canonical_submission_phases.md)
- [SenderAuthorizationKind](../../../../../classes/crates/lpe-storage/src/submission/types/SenderAuthorizationKind.md)
- [as_str](../../../../../functions/crates/lpe-storage/src/submission/types/SenderAuthorizationKind/as_str.md)
- [SenderDelegationRight](../../../../../classes/crates/lpe-storage/src/submission/types/SenderDelegationRight.md)
- [as_str](../../../../../functions/crates/lpe-storage/src/submission/types/SenderDelegationRight/as_str.md)
- [MailboxAccountAccess](../../../../../classes/crates/lpe-storage/src/submission/types/MailboxAccountAccess.md)
- [SenderIdentity](../../../../../classes/crates/lpe-storage/src/submission/types/SenderIdentity.md)
- [MailboxDelegationGrantInput](../../../../../classes/crates/lpe-storage/src/submission/types/MailboxDelegationGrantInput.md)
- [MailboxFolderDelegationGrantInput](../../../../../classes/crates/lpe-storage/src/submission/types/MailboxFolderDelegationGrantInput.md)
- [SenderDelegationGrantInput](../../../../../classes/crates/lpe-storage/src/submission/types/SenderDelegationGrantInput.md)
- [MailboxDelegationGrant](../../../../../classes/crates/lpe-storage/src/submission/types/MailboxDelegationGrant.md)
- [SenderDelegationGrant](../../../../../classes/crates/lpe-storage/src/submission/types/SenderDelegationGrant.md)
- [MailboxDelegationOverview](../../../../../classes/crates/lpe-storage/src/submission/types/MailboxDelegationOverview.md)
- [AccountIdentity](../../../../../classes/crates/lpe-storage/src/submission/types/AccountIdentity.md)
- [ResolvedSubmissionAuthorization](../../../../../classes/crates/lpe-storage/src/submission/types/ResolvedSubmissionAuthorization.md)
- [map_mailbox_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/types/map_mailbox_delegation_grant.md)
- [map_sender_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/types/map_sender_delegation_grant.md)
- [validate_mailbox_delegation_rights](../../../../../functions/crates/lpe-storage/src/submission/types/validate_mailbox_delegation_rights.md)
- [normalize_visible_recipients](../../../../../functions/crates/lpe-storage/src/submission/types/normalize_visible_recipients.md)
- [normalize_bcc_recipients](../../../../../functions/crates/lpe-storage/src/submission/types/normalize_bcc_recipients.md)
- [push_recipients](../../../../../functions/crates/lpe-storage/src/submission/types/push_recipients.md)
- [push_bcc_recipients](../../../../../functions/crates/lpe-storage/src/submission/types/push_bcc_recipients.md)
- [participants_normalized](../../../../../functions/crates/lpe-storage/src/submission/types/participants_normalized.md)
- [sender_authorization_kind_from_str](../../../../../functions/crates/lpe-storage/src/submission/types/sender_authorization_kind_from_str.md)
- [sender_identity_id](../../../../../functions/crates/lpe-storage/src/submission/types/sender_identity_id.md)
- [submission_authorization_kind_sql](../../../../../functions/crates/lpe-storage/src/submission/types/submission_authorization_kind_sql.md)
- [source_protocol_sql](../../../../../functions/crates/lpe-storage/src/submission/types/source_protocol_sql.md)
- [canonical_submission_persists_sent_before_queue_handoff](../../../../../functions/crates/lpe-storage/src/submission/types/canonical_submission_persists_sent_before_queue_handoff.md)
- [draft_submission_deletes_source_only_after_queue_persistence](../../../../../functions/crates/lpe-storage/src/submission/types/draft_submission_deletes_source_only_after_queue_persistence.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `uuid::Uuid`
- `crate::{normalize_email, MailboxDelegationGrantRow, SenderDelegationGrantRow}`
- `super::{canonical_submission_phases, CanonicalSubmissionPhase}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)