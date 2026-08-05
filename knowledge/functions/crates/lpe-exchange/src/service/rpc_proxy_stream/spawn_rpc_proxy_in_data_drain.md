---
type: Rust Function
title: spawn_rpc_proxy_in_data_drain
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L232-L394
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
---

# Signature

`pub(super) fn spawn_rpc_proxy_in_data_drain<S, V>( store: S, validator: Validator<V>, principal: AccountPrincipal, method: &Method, uri: &Uri, headers: &HeaderMap, body: Body, ) where S: ExchangeStore + Send + Sync + 'static, V: Detector + Send + Sync + 'static,`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response.md)
- [log_and_forward_rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response.md)

# Called by

- [handle_rpc_proxy_in_data_channel](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)