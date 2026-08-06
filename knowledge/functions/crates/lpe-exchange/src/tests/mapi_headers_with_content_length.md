---
type: Rust Function
title: mapi_headers_with_content_length
resource: crates/lpe-exchange/src/tests/mod.rs#L12515-L12522
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_content_length_with_parseable_error
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_rejects_nonzero_content_length
---

# Signature

`fn mapi_headers_with_content_length(request_type: &str, content_length: &'static str) -> HeaderMap`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)

# Called by

- [mapi_over_http_rejects_invalid_content_length_with_parseable_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_content_length_with_parseable_error.md)
- [mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation.md)
- [mapi_over_http_ping_rejects_nonzero_content_length](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_rejects_nonzero_content_length.md)