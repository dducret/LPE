---
type: Rust Function
title: mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L727-L756
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`async fn mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header_with_mismatched_sequence](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header_with_mismatched_sequence.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)