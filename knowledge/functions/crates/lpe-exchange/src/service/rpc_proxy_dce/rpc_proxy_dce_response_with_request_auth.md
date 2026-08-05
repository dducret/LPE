---
type: Rust Function
title: rpc_proxy_dce_response_with_request_auth
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L162-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn rpc_proxy_dce_response_with_request_auth( mut response: Vec<u8>, request: &[u8], ) -> Vec<u8>`

# Calls

- [rpc_proxy_dce_request_auth](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)