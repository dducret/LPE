---
type: Rust Function
title: rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L967-L1007
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack()`

# Calls

- [mark_rpc_proxy_out_endpoint_bind_ack](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack.md)
- [rpc_proxy_in_channel_response_for_endpoint_query](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)