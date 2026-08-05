---
type: Rust Function
title: is_rpc_proxy_in_data_channel_request
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L12-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_ping
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_zero_length_request
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_handler
---

# Signature

`pub(crate) fn is_rpc_proxy_in_data_channel_request( method: &Method, uri: &Uri, headers: &HeaderMap, ) -> bool`

# Calls

- [is_rpc_proxy_endpoint_ping](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_ping.md)
- [is_rpc_proxy_msrpc_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request.md)
- [is_rpc_proxy_zero_length_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_zero_length_request.md)

# Called by

- [rpc_proxy_handler](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)