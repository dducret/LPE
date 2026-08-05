---
type: Rust Function
title: log_and_forward_rpc_proxy_in_channel_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L397-L448
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/send_rpc_proxy_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain
---

# Signature

`fn log_and_forward_rpc_proxy_in_channel_response( method: &str, path: &str, query: &str, trace_id: &str, client_request_id: &str, x_request_id: &str, user_agent: &str, started_at: Instant, virtual_connection_cookie: &mut Option<[u8; 16]>, response: RpcProxyInChannelResponse, )`

# Calls

- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [send_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/send_rpc_proxy_out_channel.md)
- [queue_pending_rpc_proxy_out_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response.md)

# Called by

- [spawn_rpc_proxy_in_data_drain](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain.md)