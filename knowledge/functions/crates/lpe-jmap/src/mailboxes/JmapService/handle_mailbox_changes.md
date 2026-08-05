---
type: Rust Method
title: handle_mailbox_changes
resource: crates/lpe-jmap/src/mailboxes.rs#L272-L319
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries
  - functions/crates/lpe-jmap/src/state/state_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_mailbox_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries.md)
- [state_cursor](../../../../../../functions/crates/lpe-jmap/src/state/state_cursor.md)
- [changes_response_from_durable_with_cursor](../../../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)