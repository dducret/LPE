---
type: Rust Method
title: handle_email_copy
resource: crates/lpe-jmap/src/mail.rs#L381-L464
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write
  - functions/crates/lpe-jmap/src/drafts/parse_email_copy
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write
  - functions/crates/lpe-jmap/src/error/set_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_copy( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_account_may_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [mail_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)
- [ensure_mailbox_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write.md)
- [parse_email_copy](../../../../../../functions/crates/lpe-jmap/src/drafts/parse_email_copy.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)