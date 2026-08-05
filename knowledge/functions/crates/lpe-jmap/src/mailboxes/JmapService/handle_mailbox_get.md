---
type: Rust Method
title: handle_mailbox_get
resource: crates/lpe-jmap/src/mailboxes.rs#L30-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
---

# Signature

`pub(crate) async fn handle_mailbox_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_properties](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [mailbox_to_value](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value.md)
- [mailbox_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [benchmark_mailbox_listing_and_push_paths](../../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)