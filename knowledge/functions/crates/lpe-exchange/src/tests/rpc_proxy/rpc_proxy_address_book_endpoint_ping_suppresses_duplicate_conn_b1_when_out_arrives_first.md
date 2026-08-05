---
type: Rust Function
title: rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L263-L307
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
---

# Signature

`async fn rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first()`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [rpc_proxy_conn_a1_request_body](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body.md)
- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
- [handle_rpc_proxy_in_data_channel](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)