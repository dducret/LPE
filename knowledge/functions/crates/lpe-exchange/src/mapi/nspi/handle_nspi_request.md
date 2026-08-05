---
type: Rust Function
title: handle_nspi_request
resource: crates/lpe-exchange/src/mapi/nspi.rs#L52-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/bind_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_u32_result_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_info_response
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_disabled_mutation_response
  - functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_update_stat_response
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) async fn handle_nspi_request<S>( store: &S, principal: &AccountPrincipal, headers: &HeaderMap, request: &[u8], request_type: MapiRequestType, request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [bind_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [disconnect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [nspi_u32_result_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_u32_result_response.md)
- [nspi_dn_to_mid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_matches_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_get_prop_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_hierarchy_info_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_info_response.md)
- [nspi_special_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_response.md)
- [nspi_template_info_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
- [nspi_disabled_mutation_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_disabled_mutation_response.md)
- [endpoint_url_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response.md)
- [nspi_property_tags_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response.md)
- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
- [nspi_update_stat_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_update_stat_response.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)

# Called by

- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)