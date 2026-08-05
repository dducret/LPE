---
type: Rust Method
title: requested_account_access
resource: crates/lpe-jmap/src/service.rs#L304-L319
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_quota_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_search_snippet_get
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
---

# Signature

`pub(crate) async fn requested_account_access( &self, account: &AuthenticatedAccount, requested_account_id: Option<&str>, ) -> Result<MailboxAccountAccess>`

# Calls

- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [handle_blob_upload](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload.md)
- [handle_blob_get](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get.md)
- [handle_blob_lookup](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup.md)
- [handle_blob_copy](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)
- [handle_email_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_email_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get.md)
- [handle_email_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes.md)
- [handle_email_copy](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [handle_email_set](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [handle_email_submission_set](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_email_submission_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [handle_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_email_submission_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)
- [handle_email_submission_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes.md)
- [handle_identity_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)
- [handle_identity_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes.md)
- [handle_thread_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query.md)
- [handle_thread_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes.md)
- [handle_thread_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)
- [handle_thread_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes.md)
- [handle_quota_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_quota_get.md)
- [handle_search_snippet_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_search_snippet_get.md)
- [handle_mailbox_get](../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)
- [handle_mailbox_query](../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_mailbox_query_changes](../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_mailbox_changes](../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes.md)
- [handle_mailbox_set](../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [handle_upload](../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload.md)
- [handle_download](../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download.md)
- [canonical_object_state](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)