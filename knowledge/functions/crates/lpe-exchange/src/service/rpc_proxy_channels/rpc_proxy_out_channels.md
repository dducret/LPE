---
type: Rust Function
title: rpc_proxy_out_channels
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L190-L193
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/register_rpc_proxy_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/send_rpc_proxy_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/remove_rpc_proxy_out_channel
---

# Signature

`fn rpc_proxy_out_channels() -> &'static Mutex<HashMap<OutChannelKey, OutChannelSender>>`

# Called by

- [register_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/register_rpc_proxy_out_channel.md)
- [send_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/send_rpc_proxy_out_channel.md)
- [remove_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/remove_rpc_proxy_out_channel.md)