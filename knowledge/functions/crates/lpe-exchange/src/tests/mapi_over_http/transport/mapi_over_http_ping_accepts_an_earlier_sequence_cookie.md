---
type: Rust Function
title: mapi_over_http_ping_accepts_an_earlier_sequence_cookie
resource: crates/lpe-exchange/src/tests/mapi_over_http/transport.rs#L1262-L1285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
---

# Signature

`async fn mapi_over_http_ping_accepts_an_earlier_sequence_cookie()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header_with_mismatched_sequence](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)