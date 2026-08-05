---
type: Rust Function
title: rpc_proxy_rfri_get_fqdn_response
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L336-L344
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_referral_server_name
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
---

# Signature

`pub(super) fn rpc_proxy_rfri_get_fqdn_response(call_id: u32, endpoint_query: &str) -> Vec<u8>`

# Calls

- [rpc_proxy_referral_server_name](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_referral_server_name.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_ndr_ascii_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)