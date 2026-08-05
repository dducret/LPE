---
type: Rust Function
title: mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect
resource: crates/lpe-exchange/src/tests/mapi_over_http/transport.rs#L1703-L1740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence
---

# Signature

`async fn mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header_with_mismatched_sequence](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence.md)