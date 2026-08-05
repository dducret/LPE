---
type: Rust Function
title: is_rpc_proxy_msrpc_request
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L39-L47
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_echo_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request
---

# Signature

`pub(super) fn is_rpc_proxy_msrpc_request(headers: &HeaderMap) -> bool`

# Called by

- [is_rpc_proxy_echo_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_echo_request.md)
- [is_rpc_proxy_in_data_channel_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request.md)
- [parse_rpc_proxy_out_data_connect_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request.md)