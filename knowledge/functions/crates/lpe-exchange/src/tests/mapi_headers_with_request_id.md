---
type: Rust Function
title: mapi_headers_with_request_id
resource: crates/lpe-exchange/src/tests/mod.rs#L12491-L12495
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_maps_response_code_to_header_and_envelope
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_request_id_with_parseable_error
---

# Signature

`fn mapi_headers_with_request_id(request_type: &str, request_id: &'static str) -> HeaderMap`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)

# Called by

- [mapi_over_http_transport_maps_response_code_to_header_and_envelope](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_maps_response_code_to_header_and_envelope.md)
- [mapi_over_http_rejects_invalid_request_id_with_parseable_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_request_id_with_parseable_error.md)