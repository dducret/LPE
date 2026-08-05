---
type: Rust Function
title: rpc_proxy_push_property_tag_array
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L869-L877
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
---

# Signature

`fn rpc_proxy_push_property_tag_array(buffer: &mut Vec<u8>, values: &[u32])`

# Calls

- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)

# Called by

- [rpc_proxy_nspi_get_matches_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response.md)
- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_resort_restriction_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response.md)
- [rpc_proxy_nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response.md)
- [rpc_proxy_nspi_property_tags_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response.md)
- [rpc_proxy_nspi_resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)