---
type: Rust Method
title: handle_mailbox_query
resource: crates/lpe-jmap/src/mailboxes.rs#L75-L158
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mailboxes/filter_mailboxes
  - functions/crates/lpe-jmap/src/state/query_position
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
  - functions/crates/lpe-jmap/src/state/encode_query_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
---

# Signature

`pub(crate) async fn handle_mailbox_query( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [filter_mailboxes](../../../../../../functions/crates/lpe-jmap/src/mailboxes/filter_mailboxes.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)
- [encode_query_state_reference](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [benchmark_mailbox_listing_and_push_paths](../../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)