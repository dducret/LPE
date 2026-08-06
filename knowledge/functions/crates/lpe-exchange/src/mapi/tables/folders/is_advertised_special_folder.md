---
type: Rust Function
title: is_advertised_special_folder
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L134-L156
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/rop/debug/expected_folder_type_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id
  - functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree
---

# Signature

`pub(in crate::mapi) fn is_advertised_special_folder(folder_id: u64) -> bool`

# Calls

- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [debug_object_scope_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [expected_folder_type_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/expected_folder_type_for_debug.md)
- [format_folder_type_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_long_term_id.md)
- [stale_special_folder_object_id_from_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)
- [advertised_virtual_object_id_from_bare_little_endian_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id.md)
- [unresolved_mapi_object_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [is_expected_unbacked_mapi_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)
- [hierarchy_folder_is_in_ipm_subtree](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree.md)