---
type: Rust Function
title: rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L360-L389
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
---

# Signature

`async fn rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof()`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [handle_rpc_proxy_in_data_channel](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)