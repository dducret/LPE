---
type: Rust Function
title: stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes
resource: crates/lpe-jmap/src/tests.rs#L4634-L4746
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/validator_ok
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/state/decode_query_state
---

# Signature

`async fn stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes()`

# Calls

- [validator_ok](../../../../../functions/crates/lpe-jmap/src/tests/validator_ok.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [decode_query_state](../../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)