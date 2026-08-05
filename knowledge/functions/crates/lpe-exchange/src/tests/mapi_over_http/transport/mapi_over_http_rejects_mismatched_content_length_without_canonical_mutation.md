---
type: Rust Function
title: mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation
resource: crates/lpe-exchange/src/tests/mapi_over_http/transport.rs#L865-L898
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
  - functions/crates/lpe-exchange/src/tests/mapi_headers_with_content_length
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`async fn mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [mapi_submit_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)
- [mapi_headers_with_content_length](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_with_content_length.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)