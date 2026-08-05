---
type: Rust Function
title: consume_rpc_proxy_out_endpoint_rts_connect
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L148-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_rts_connects
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response
---

# Signature

`pub(super) fn consume_rpc_proxy_out_endpoint_rts_connect(query: &str) -> bool`

# Calls

- [rpc_proxy_out_endpoint_rts_connects](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_rts_connects.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [rpc_proxy_in_channel_response_for_endpoint_query_with_store_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response.md)