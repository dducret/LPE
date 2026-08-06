---
type: Rust Function
title: mailbox_account_may_submit
resource: crates/lpe-jmap/src/mailboxes.rs#L544-L546
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) fn mailbox_account_may_submit(access: &MailboxAccountAccess) -> bool`

# Called by

- [handle_email_submission_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_email_submission_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [handle_email_submission_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_email_submission_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)
- [handle_email_submission_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes.md)
- [handle_identity_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)
- [handle_identity_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes.md)
- [mailbox_account_may_draft](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft.md)
- [ensure_mailbox_draft_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)
- [handle_canonical_query](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [mailbox_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)
- [mail_push_type_state](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)