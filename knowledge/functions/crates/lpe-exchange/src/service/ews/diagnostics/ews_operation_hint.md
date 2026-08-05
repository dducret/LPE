---
type: Rust Function
title: ews_operation_hint
resource: crates/lpe-exchange/src/service/ews/diagnostics.rs#L9-L13
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/decode_ews_body
  - functions/crates/lpe-exchange/src/service/ews/xml/operation_name
  called_by:
  - functions/crates/lpe-exchange/src/service/post_handler
---

# Signature

`pub(in crate::service) fn ews_operation_hint(headers: &HeaderMap, body: &[u8]) -> Option<String>`

# Calls

- [decode_ews_body](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/decode_ews_body.md)
- [operation_name](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/operation_name.md)

# Called by

- [post_handler](../../../../../../../functions/crates/lpe-exchange/src/service/post_handler.md)