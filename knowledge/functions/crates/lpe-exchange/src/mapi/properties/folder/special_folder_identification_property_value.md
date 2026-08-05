---
type: Rust Function
title: special_folder_identification_property_value
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L62-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/valid_folder_mask
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id_value
  - functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids_ex
  - functions/crates/lpe-exchange/src/mapi/properties/folder/free_busy_entry_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_indexed_special_folder_entry_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_identification_contract_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tests/additional_ren_entry_ids_ex_advertises_outlook_store_special_folders
  - functions/crates/lpe-exchange/src/mapi/properties/tests/additional_ren_entry_ids_advertises_documented_indexed_special_folders
  - functions/crates/lpe-exchange/src/mapi/properties/tests/free_busy_entry_ids_advertises_freebusy_data_at_documented_index
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
---

# Signature

`pub(in crate::mapi) fn special_folder_identification_property_value( mailbox_guid: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [valid_folder_mask](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/valid_folder_mask.md)
- [special_folder_entry_id_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id_value.md)
- [additional_ren_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids.md)
- [additional_ren_entry_ids_ex](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids_ex.md)
- [free_busy_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/free_busy_entry_ids.md)

# Called by

- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)
- [merge_indexed_special_folder_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_indexed_special_folder_entry_ids.md)
- [merge_additional_ren_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids.md)
- [default_folder_identification_contract_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_identification_contract_for_debug.md)
- [default_folder_hierarchy_projection_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug.md)
- [default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [additional_ren_entry_ids_ex_advertises_outlook_store_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/additional_ren_entry_ids_ex_advertises_outlook_store_special_folders.md)
- [additional_ren_entry_ids_advertises_documented_indexed_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/additional_ren_entry_ids_advertises_documented_indexed_special_folders.md)
- [free_busy_entry_ids_advertises_freebusy_data_at_documented_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/free_busy_entry_ids_advertises_freebusy_data_at_documented_index.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [semantic_property_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)
- [log_calendar_default_folder_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [special_folder_property_value_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)