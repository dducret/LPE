---
type: Rust Function
title: object_changes_with_cursor_do_not_diff_unlogged_current_state
resource: crates/lpe-jmap/src/tests.rs#L12999-L13064
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn object_changes_with_cursor_do_not_diff_unlogged_current_state()`

# Calls

- [encode_state_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)