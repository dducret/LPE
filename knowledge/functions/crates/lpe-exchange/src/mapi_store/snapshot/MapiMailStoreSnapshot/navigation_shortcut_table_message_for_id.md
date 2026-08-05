---
type: Rust Method
title: navigation_shortcut_table_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1304-L1309
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/navigation_shortcut_message_for_open
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table
---

# Signature

`pub(crate) fn navigation_shortcut_table_message_for_id( &self, item_id: u64, ) -> Option<MapiNavigationShortcutMessage>`

# Calls

- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)

# Called by

- [navigation_shortcut_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/navigation_shortcut_message_for_open.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)