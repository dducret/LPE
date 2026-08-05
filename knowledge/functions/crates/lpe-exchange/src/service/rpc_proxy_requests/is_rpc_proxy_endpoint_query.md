---
type: Rust Function
title: is_rpc_proxy_endpoint_query
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L35-L37
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/should_hold_rpc_proxy_in_channel
---

# Signature

`pub(super) fn is_rpc_proxy_endpoint_query(query: &str) -> bool`

# Called by

- [should_hold_rpc_proxy_in_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/should_hold_rpc_proxy_in_channel.md)