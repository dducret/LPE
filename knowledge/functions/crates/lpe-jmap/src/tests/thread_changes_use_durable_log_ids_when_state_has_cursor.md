---
type: Rust Function
title: thread_changes_use_durable_log_ids_when_state_has_cursor
resource: crates/lpe-jmap/src/tests.rs#L4331-L4376
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn thread_changes_use_durable_log_ids_when_state_has_cursor()`

# Calls

- [encode_state_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)