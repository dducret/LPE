---
type: Rust Function
title: pending_rpc_proxy_out_channel_responses
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L183-L188
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_pending_rpc_proxy_out_channel_responses
---

# Signature

`fn pending_rpc_proxy_out_channel_responses( ) -> &'static Mutex<HashMap<String, Vec<PendingOutChannelResponse>>>`

# Called by

- [queue_pending_rpc_proxy_out_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response.md)
- [consume_pending_rpc_proxy_out_channel_responses](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_pending_rpc_proxy_out_channel_responses.md)