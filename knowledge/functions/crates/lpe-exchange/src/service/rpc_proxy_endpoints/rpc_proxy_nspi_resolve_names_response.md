---
type: Rust Function
title: rpc_proxy_nspi_resolve_names_response
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L621-L672
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_display_name_for_smtp_address
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum
---

# Signature

`fn rpc_proxy_nspi_resolve_names_response(call_id: u32, request: &[u8]) -> Vec<u8>`

# Calls

- [rpc_proxy_nspi_requested_smtp_address](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address.md)
- [rpc_proxy_display_name_for_smtp_address](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_display_name_for_smtp_address.md)
- [rpc_proxy_nspi_requested_resolve_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array.md)
- [rpc_proxy_push_ndr_ascii_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)

# Called by

- [rpc_proxy_nspi_response_for_opnum](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum.md)