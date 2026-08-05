---
type: Rust Function
title: rpc_proxy_handler
resource: crates/lpe-exchange/src/service.rs#L295-L351
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`async fn rpc_proxy_handler( State(storage): State<Storage>, method: Method, uri: Uri, headers: HeaderMap, body: Body, ) -> Response`

# Calls

- [is_rpc_proxy_in_data_channel_request](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request.md)
- [handle_rpc_proxy_in_data_channel](../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)
- [log_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [handle_rpc_proxy](../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)