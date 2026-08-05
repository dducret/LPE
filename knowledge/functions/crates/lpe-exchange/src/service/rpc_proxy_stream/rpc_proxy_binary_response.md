---
type: Rust Function
title: rpc_proxy_binary_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L156-L174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_echo_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`fn rpc_proxy_binary_response(body: Vec<u8>, status: &'static str) -> Response`

# Calls

- [rpc_proxy_held_open_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response.md)
- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [decorate_rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response.md)

# Called by

- [rpc_proxy_rts_connect_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response.md)
- [rpc_proxy_echo_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_echo_response.md)
- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)