---
type: Rust Function
title: rpc_proxy_in_channel_response_for_endpoint_query_with_store_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L563-L633
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_rts_connect
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store
---

# Signature

`async fn rpc_proxy_in_channel_response_for_endpoint_query_with_store_response<S, V>( store: &S, validator: &Validator<V>, principal: &AccountPrincipal, endpoint_query: &str, buffer: &mut Vec<u8>, ) -> Option<RpcProxyInChannelResponse> where S: ExchangeStore, V: Detector,`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rpc_proxy_conn_b1_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body.md)
- [consume_rpc_proxy_out_endpoint_rts_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_rts_connect.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
- [rpc_proxy_address_book_check_name_fallback](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback.md)

# Called by

- [spawn_rpc_proxy_in_data_drain](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store.md)