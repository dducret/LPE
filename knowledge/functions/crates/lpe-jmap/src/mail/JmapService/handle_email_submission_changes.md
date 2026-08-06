---
type: Rust Method
title: handle_email_submission_changes
resource: crates/lpe-jmap/src/mail.rs#L877-L901
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_submission_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [email_submission_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries.md)
- [object_changes_response](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)