---
type: Rust Function
title: should_hold_rpc_proxy_in_channel
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L142-L154
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_query
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response
---

# Signature

`fn should_hold_rpc_proxy_in_channel(uri: &Uri) -> bool`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [is_rpc_proxy_endpoint_query](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_query.md)

# Called by

- [rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)