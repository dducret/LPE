---
type: Rust Function
title: email_state_tokens_do_not_expose_message_or_bcc_content
resource: crates/lpe-jmap/src/tests.rs#L3980-L4023
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/state/decode_state
---

# Signature

`async fn email_state_tokens_do_not_expose_message_or_bcc_content()`

# Calls

- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [decode_state](../../../../../functions/crates/lpe-jmap/src/state/decode_state.md)