---
type: Rust Function
title: rpc_proxy_address_book_check_name_fallback
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L500-L520
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_last_dce_request_call_id
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response
---

# Signature

`async fn rpc_proxy_address_book_check_name_fallback<S>( store: &S, endpoint_query: &str, buffer: &[u8], principal: &AccountPrincipal, ) -> Option<RpcProxyInChannelResponse> where S: ExchangeStore,`

# Calls

- [rpc_proxy_nspi_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)
- [rpc_proxy_last_dce_request_call_id](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_last_dce_request_call_id.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)

# Called by

- [rpc_proxy_in_channel_response_for_endpoint_query_with_store_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response.md)