---
type: Rust Method
title: handle_email_submission_set
resource: crates/lpe-jmap/src/mail.rs#L647-L727
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
  - functions/crates/lpe-jmap/src/parse/parse_submission_email_id
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/error/method_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_submission_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [email_submission_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
- [parse_submission_email_id](../../../../../../functions/crates/lpe-jmap/src/parse/parse_submission_email_id.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [method_error](../../../../../../functions/crates/lpe-jmap/src/error/method_error.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)