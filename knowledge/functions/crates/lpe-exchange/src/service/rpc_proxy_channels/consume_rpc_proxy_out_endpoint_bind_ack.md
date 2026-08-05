---
type: Rust Function
title: consume_rpc_proxy_out_endpoint_bind_ack
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L36-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_bind_acks
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn consume_rpc_proxy_out_endpoint_bind_ack(query: &str) -> bool`

# Calls

- [rpc_proxy_out_endpoint_bind_acks](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_bind_acks.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)