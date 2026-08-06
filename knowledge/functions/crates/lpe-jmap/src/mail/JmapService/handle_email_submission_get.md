---
type: Rust Method
title: handle_email_submission_get
resource: crates/lpe-jmap/src/mail.rs#L729-L773
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/mail/values/email_submission_properties
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
  - functions/crates/lpe-jmap/src/state/encode_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_submission_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [email_submission_properties](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_properties.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [email_submission_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
- [encode_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)