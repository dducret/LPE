---
type: Rust Function
title: rpc_proxy_held_open_binary_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L176-L203
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
---

# Signature

`fn rpc_proxy_held_open_binary_response( body: Vec<u8>, status: &'static str, hold_open_ms: u64, send_initial_body: bool, include_content_length: bool, ) -> Response`

# Calls

- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [decorate_rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)
- [rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)