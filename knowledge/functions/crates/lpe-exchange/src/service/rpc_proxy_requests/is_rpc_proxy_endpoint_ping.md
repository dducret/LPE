---
type: Rust Function
title: is_rpc_proxy_endpoint_ping
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L31-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request
---

# Signature

`pub(super) fn is_rpc_proxy_endpoint_ping(uri: &Uri) -> bool`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
- [is_rpc_proxy_in_data_channel_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request.md)