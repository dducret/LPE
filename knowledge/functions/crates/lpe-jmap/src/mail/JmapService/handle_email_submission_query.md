---
type: Rust Method
title: handle_email_submission_query
resource: crates/lpe-jmap/src/mail.rs#L775-L834
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mail/values/validate_email_submission_query
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query
  - functions/crates/lpe-jmap/src/state/query_position
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_submission_query( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [validate_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/values/validate_email_submission_query.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [apply_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)