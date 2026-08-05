---
type: Rust Function
title: push_le_u32
resource: crates/lpe-exchange/src/service/rpc_proxy_codec.rs#L42-L44
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_byte_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_utf16_string
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal
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
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row
---

# Signature

`pub(super) fn push_le_u32(buffer: &mut Vec<u8>, value: u32)`

# Called by

- [rpc_proxy_push_ndr_byte_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_byte_array.md)
- [rpc_proxy_push_ndr_ascii_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string.md)
- [rpc_proxy_push_ndr_utf16_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_utf16_string.md)
- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer.md)
- [rpc_proxy_emsmdb_disconnect_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response.md)
- [rpc_proxy_rfri_get_new_dsa_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_new_dsa_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
- [rpc_proxy_rfri_get_fqdn_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal.md)
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
- [rpc_proxy_push_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array.md)
- [rpc_proxy_push_stat](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat.md)
- [rpc_proxy_push_rowset_pointer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer.md)
- [rpc_proxy_push_property_row](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row.md)