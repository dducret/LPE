---
type: Rust Function
title: mark_rpc_proxy_out_endpoint_rts_connect
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L140-L146
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_rts_connects
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn mark_rpc_proxy_out_endpoint_rts_connect(query: &str)`

# Calls

- [rpc_proxy_out_endpoint_rts_connects](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_rts_connects.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)