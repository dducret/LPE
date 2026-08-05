---
type: Rust Function
title: rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L202-L260
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`async fn rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first()`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [handle_rpc_proxy_in_data_channel](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)
- [rpc_proxy_conn_a1_request_body](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body.md)
- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)