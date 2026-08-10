---
type: Rust Function
title: thread_query_state_keeps_full_snapshot_when_page_is_limited
resource: crates/lpe-jmap/src/tests.rs#L6190-L6233
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/tests/validator_ok
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/state/decode_query_state
---

# Signature

`async fn thread_query_state_keeps_full_snapshot_when_page_is_limited()`

# Calls

- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [validator_ok](../../../../../functions/crates/lpe-jmap/src/tests/validator_ok.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [decode_query_state](../../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)