---
type: Rust Function
title: rpc_proxy_out_endpoint_bind_acks
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L173-L176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_bind_ack
---

# Signature

`fn rpc_proxy_out_endpoint_bind_acks() -> &'static Mutex<HashMap<String, usize>>`

# Called by

- [mark_rpc_proxy_out_endpoint_bind_ack](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack.md)
- [consume_rpc_proxy_out_endpoint_bind_ack](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_bind_ack.md)