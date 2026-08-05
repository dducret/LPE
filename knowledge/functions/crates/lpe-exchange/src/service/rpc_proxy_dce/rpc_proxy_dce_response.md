---
type: Rust Function
title: rpc_proxy_dce_response
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L183-L197
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_mgmt_inq_stats_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_bind_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_unbind_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_update_stat_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_names_from_ids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_compare_mids_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_special_table_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
---

# Signature

`pub(super) fn rpc_proxy_dce_response(call_id: u32, stub: &[u8]) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_mgmt_inq_stats_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_mgmt_inq_stats_response.md)
- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer.md)
- [rpc_proxy_emsmdb_disconnect_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response.md)
- [rpc_proxy_rfri_get_new_dsa_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_new_dsa_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
- [rpc_proxy_rfri_get_fqdn_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal.md)
- [rpc_proxy_nspi_bind_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_bind_response.md)
- [rpc_proxy_nspi_unbind_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_unbind_response.md)
- [rpc_proxy_nspi_update_stat_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_update_stat_response.md)
- [rpc_proxy_nspi_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response.md)
- [rpc_proxy_nspi_query_rows_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal.md)
- [rpc_proxy_nspi_get_matches_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response.md)
- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_resort_restriction_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response.md)
- [rpc_proxy_nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response.md)
- [rpc_proxy_nspi_property_tags_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response.md)
- [rpc_proxy_nspi_get_names_from_ids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_names_from_ids_response.md)
- [rpc_proxy_nspi_get_props_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response.md)
- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)
- [rpc_proxy_nspi_compare_mids_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_compare_mids_response.md)
- [rpc_proxy_nspi_get_special_table_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_special_table_response.md)
- [rpc_proxy_nspi_resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)