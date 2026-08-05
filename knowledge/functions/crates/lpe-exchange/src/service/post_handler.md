---
type: Rust Function
title: post_handler
resource: crates/lpe-exchange/src/service.rs#L187-L226
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/log_ews_connection
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_code
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_debug_detail
---

# Signature

`async fn post_handler( State(storage): State<Storage>, uri: Uri, headers: HeaderMap, body: Bytes, ) -> Response`

# Calls

- [ews_operation_hint](../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint.md)
- [log_ews_connection](../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/log_ews_connection.md)
- [ews_response_code](../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_code.md)
- [ews_response_debug_detail](../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_debug_detail.md)