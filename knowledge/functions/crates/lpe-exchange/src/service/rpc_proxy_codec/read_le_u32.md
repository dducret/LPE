---
type: Rust Function
title: read_le_u32
resource: crates/lpe-exchange/src/service/rpc_proxy_codec.rs#L1-L4
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_known_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_mids
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_last_dce_request_call_id
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn read_le_u32(body: &[u8], offset: usize) -> Option<u32>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rpc_proxy_dce_request_auth](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth.md)
- [rpc_proxy_nspi_known_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_known_property_tags.md)
- [rpc_proxy_nspi_requested_resolve_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags.md)
- [rpc_proxy_nspi_requested_mids](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_mids.md)
- [parse_rpc_rts_u32_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command.md)
- [parse_rpc_rts_cookie_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command.md)
- [rpc_proxy_last_dce_request_call_id](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_last_dce_request_call_id.md)
- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)