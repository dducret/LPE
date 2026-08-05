---
type: Rust Function
title: mark_rpc_proxy_out_endpoint_bind_ack
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L28-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_bind_acks
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack
---

# Signature

`pub(crate) fn mark_rpc_proxy_out_endpoint_bind_ack(query: &str)`

# Calls

- [rpc_proxy_out_endpoint_bind_acks](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_bind_acks.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
- [rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack.md)