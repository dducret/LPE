---
type: Rust Function
title: format_folder_type_getprops_contract
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L1282-L1394
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/debug/expected_folder_type_for_debug
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_reports_loaded_inbox
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_flags_inbox_without_snapshot
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_accepts_advertised_search_folder
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_prefers_saved_search_definition
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_accepts_projected_search_folder_role
---

# Signature

`pub(in crate::mapi) fn format_folder_type_getprops_contract( object: Option<&MapiObject>, principal: &AccountPrincipal, columns: &[u32], mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [collaboration_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [public_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)
- [expected_folder_type_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/expected_folder_type_for_debug.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [folder_type_getprops_contract_reports_loaded_inbox](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_reports_loaded_inbox.md)
- [folder_type_getprops_contract_flags_inbox_without_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_flags_inbox_without_snapshot.md)
- [folder_type_getprops_contract_accepts_advertised_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_accepts_advertised_search_folder.md)
- [folder_type_getprops_contract_prefers_saved_search_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_prefers_saved_search_definition.md)
- [folder_type_getprops_contract_accepts_projected_search_folder_role](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_accepts_projected_search_folder_role.md)