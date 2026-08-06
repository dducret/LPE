---
type: Rust Function
title: mapi_cookie_header_with_mismatched_sequence
resource: crates/lpe-exchange/src/tests/mod.rs#L12711-L12723
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_accepts_an_earlier_sequence_cookie
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect
---

# Signature

`fn mapi_cookie_header_with_mismatched_sequence(response: &axum::response::Response) -> String`

# Calls

- [mapi_cookie_header](../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)

# Called by

- [mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie.md)
- [mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect.md)
- [mapi_over_http_ping_accepts_an_earlier_sequence_cookie](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_accepts_an_earlier_sequence_cookie.md)
- [mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect.md)