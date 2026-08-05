---
type: Rust Method
title: handle_identity_changes
resource: crates/lpe-jmap/src/mail.rs#L947-L970
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries
  - functions/crates/lpe-jmap/src/state/changes_response
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_identity_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [identity_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries.md)
- [changes_response](../../../../../../functions/crates/lpe-jmap/src/state/changes_response.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)