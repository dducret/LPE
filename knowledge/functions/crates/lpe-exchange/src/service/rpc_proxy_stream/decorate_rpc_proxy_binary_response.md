---
type: Rust Function
title: decorate_rpc_proxy_binary_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L205-L230
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response
---

# Signature

`fn decorate_rpc_proxy_binary_response( response: &mut Response, payload_bytes: usize, payload_preview_hex: String, status: &'static str, )`

# Called by

- [rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)
- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
- [rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)
- [rpc_proxy_held_open_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response.md)