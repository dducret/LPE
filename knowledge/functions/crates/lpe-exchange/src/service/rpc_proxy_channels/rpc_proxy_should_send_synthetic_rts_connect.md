---
type: Rust Function
title: rpc_proxy_should_send_synthetic_rts_connect
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L50-L52
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn rpc_proxy_should_send_synthetic_rts_connect(query: &str) -> bool`

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)