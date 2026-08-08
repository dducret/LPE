---
type: Rust Function
title: raw_response_bytes
resource: crates/lpe-exchange/src/tests/mod.rs#L12699-L12704
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_maps_response_code_to_header_and_envelope
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_response_content_length_covers_full_mapi_envelope
---

# Signature

`async fn raw_response_bytes(response: axum::response::Response) -> Vec<u8>`

# Called by

- [mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence.md)
- [mapi_over_http_transport_maps_response_code_to_header_and_envelope](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_maps_response_code_to_header_and_envelope.md)
- [mapi_over_http_response_content_length_covers_full_mapi_envelope](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_response_content_length_covers_full_mapi_envelope.md)