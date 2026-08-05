---
type: Rust Function
title: rpc_proxy_emsmdb_connect_ex_response
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L122-L126
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
---

# Signature

`pub(super) fn rpc_proxy_emsmdb_connect_ex_response(call_id: u32) -> Vec<u8>`

# Calls

- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)