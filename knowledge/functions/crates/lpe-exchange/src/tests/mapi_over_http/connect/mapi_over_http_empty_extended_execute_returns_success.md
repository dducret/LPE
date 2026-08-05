---
type: Rust Function
title: mapi_over_http_empty_extended_execute_returns_success
resource: crates/lpe-exchange/src/tests/mapi_over_http/connect.rs#L45-L75
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_wrapped_rop_buffer
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`async fn mapi_over_http_empty_extended_execute_returns_success()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [rpc_proxy_wrapped_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_wrapped_rop_buffer.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)