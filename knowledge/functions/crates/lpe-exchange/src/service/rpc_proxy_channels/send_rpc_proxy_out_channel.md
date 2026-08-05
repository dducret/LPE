---
type: Rust Function
title: send_rpc_proxy_out_channel
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L69-L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response
---

# Signature

`pub(super) fn send_rpc_proxy_out_channel( query: &str, virtual_connection_cookie: Option<[u8; 16]>, bytes: Vec<u8>, ) -> bool`

# Calls

- [rpc_proxy_out_channels](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [log_and_forward_rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response.md)