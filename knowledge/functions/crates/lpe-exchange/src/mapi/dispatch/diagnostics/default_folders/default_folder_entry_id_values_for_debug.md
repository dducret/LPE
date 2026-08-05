---
type: Rust Function
title: default_folder_entry_id_values_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L147-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/indexed_special_folder_entry_ids_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_property_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_identification_contract_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_freebusy_data_index
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_entry_id_values_for_debug( values: &[(u32, MapiValue)], ) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [default_view_entry_id_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug.md)
- [indexed_special_folder_entry_ids_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/indexed_special_folder_entry_ids_for_debug.md)
- [additional_ren_entry_ids_ex_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_for_debug.md)
- [default_folder_entry_id_expected_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id.md)
- [default_folder_entry_id_property_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_property_name.md)
- [post_hierarchy_probe_folder_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name.md)

# Called by

- [default_folder_identification_contract_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_identification_contract_for_debug.md)
- [default_folder_getprops_value_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug.md)
- [set_properties_probe_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex.md)
- [default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids.md)
- [default_folder_entry_id_values_debug_decodes_freebusy_data_index](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_freebusy_data_index.md)