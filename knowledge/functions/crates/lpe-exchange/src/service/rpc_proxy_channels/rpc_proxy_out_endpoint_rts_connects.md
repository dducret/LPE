---
type: Rust Function
title: rpc_proxy_out_endpoint_rts_connects
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L178-L181
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_rts_connect
---

# Signature

`fn rpc_proxy_out_endpoint_rts_connects() -> &'static Mutex<HashMap<String, usize>>`

# Called by

- [mark_rpc_proxy_out_endpoint_rts_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect.md)
- [consume_rpc_proxy_out_endpoint_rts_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_rts_connect.md)