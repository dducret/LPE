---
type: Rust Function
title: rpc_proxy_in_channel_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L59-L81
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/should_hold_rpc_proxy_in_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
---

# Signature

`pub(super) fn rpc_proxy_in_channel_response(uri: &Uri) -> Response`

# Calls

- [should_hold_rpc_proxy_in_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/should_hold_rpc_proxy_in_channel.md)
- [rpc_proxy_held_open_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response.md)
- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [decorate_rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response.md)

# Called by

- [handle_rpc_proxy_in_data_channel](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)