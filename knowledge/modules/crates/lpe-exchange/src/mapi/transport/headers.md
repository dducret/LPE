---
type: Rust Module
title: headers
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L1-L206
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-env
  - external/anyhow-anyhow-result
  - external/axum-http-header-content-length-content-type-set-cookie-headermap-response-response
  - external/lpe-domain-crypto-hex-lower
  - external/uuid-uuid
  - external/super-mapi-payload-fingerprint-mapirequesttype-mapi-content-type-mapi-octet-stream-content-type
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [request_type](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/request_type.md)
- [request_id](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/request_id.md)
- [is_guid_counter_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_guid_counter_header.md)
- [guid_counter_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)
- [client_flow_key](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)
- [client_info](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_info.md)
- [host_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/host_header.md)
- [content_length_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_header.md)
- [is_valid_content_length](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_valid_content_length.md)
- [content_length_matches_body](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_matches_body.md)
- [is_mapi_content_type](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_mapi_content_type.md)
- [response_set_cookie_names](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/response_set_cookie_names.md)
- [response_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/response_header.md)
- [safe_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/safe_header.md)
- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [debug_payload_preview_limit](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_limit.md)
- [hex_preview](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)

# Imports

- `std::env`
- `anyhow::{anyhow, Result}`
- `axum::{
    http::{
        header::{CONTENT_LENGTH, CONTENT_TYPE, SET_COOKIE},
        HeaderMap,
    },
    response::Response,
}`
- `lpe_domain::crypto::hex_lower`
- `uuid::Uuid`
- `super::{
    mapi_payload_fingerprint, MapiRequestType, MAPI_CONTENT_TYPE, MAPI_OCTET_STREAM_CONTENT_TYPE,
}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)