---
type: Rust Function
title: rpc_proxy_nspi_response_for_opnum
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L25-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_bind_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_unbind_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_update_stat_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_compare_mids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_special_table_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_names_from_ids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
---

# Signature

`pub(super) fn rpc_proxy_nspi_response_for_opnum( call_id: u32, opnum: u16, alloc_hint: u32, bytes: &[u8], ) -> Option<Vec<u8>>`

# Calls

- [rpc_proxy_nspi_bind_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_bind_response.md)
- [rpc_proxy_nspi_unbind_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_unbind_response.md)
- [rpc_proxy_nspi_update_stat_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_update_stat_response.md)
- [rpc_proxy_nspi_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response.md)
- [rpc_proxy_nspi_get_matches_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response.md)
- [rpc_proxy_nspi_resort_restriction_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response.md)
- [rpc_proxy_nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response.md)
- [rpc_proxy_nspi_property_tags_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response.md)
- [rpc_proxy_nspi_get_props_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response.md)
- [rpc_proxy_nspi_compare_mids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_compare_mids_response.md)
- [rpc_proxy_nspi_get_special_table_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_special_table_response.md)
- [rpc_proxy_nspi_get_names_from_ids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_names_from_ids_response.md)
- [rpc_proxy_nspi_resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)