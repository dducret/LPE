---
type: Rust Method
title: handle_mailbox_set
resource: crates/lpe-jmap/src/mailboxes.rs#L321-L469
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write
  - functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_create
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_update
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_mailbox_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [validate_mailbox_set_names](../../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)
- [mailbox_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)
- [mailbox_account_may_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [ensure_mailbox_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write.md)
- [parse_mailbox_create](../../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_create.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [parse_mailbox_update](../../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_update.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)