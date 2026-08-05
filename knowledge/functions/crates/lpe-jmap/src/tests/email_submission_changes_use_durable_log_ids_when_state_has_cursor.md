---
type: Rust Function
title: email_submission_changes_use_durable_log_ids_when_state_has_cursor
resource: crates/lpe-jmap/src/tests.rs#L5796-L5843
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/email_submission
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/state/decode_state
---

# Signature

`async fn email_submission_changes_use_durable_log_ids_when_state_has_cursor()`

# Calls

- [email_submission](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/email_submission.md)
- [encode_state_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [decode_state](../../../../../functions/crates/lpe-jmap/src/state/decode_state.md)