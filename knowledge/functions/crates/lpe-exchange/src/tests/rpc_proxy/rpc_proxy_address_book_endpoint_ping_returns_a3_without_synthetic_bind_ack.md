---
type: Rust Function
title: rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L163-L199
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`async fn rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack()`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [rpc_proxy_conn_a1_request_body](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_conn_a1_request_body.md)
- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)