---
type: Rust Function
title: special_folder_property_value
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L597-L608
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches
---

# Signature

`pub(in crate::mapi) fn special_folder_property_value( folder_id: u64, property_tag: u32, mailbox_guid: Uuid, ) -> Option<MapiValue>`

# Calls

- [special_folder_property_value_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [format_folder_type_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [inbox_getprops_captured_unpersisted_folder_values_are_absent](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent.md)
- [hierarchy_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)
- [special_hierarchy_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches.md)