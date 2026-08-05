---
type: Rust Function
title: navigation_shortcut_property_value_for_principal
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L11-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_property_value_for_principal( message: &MapiNavigationShortcutMessage, principal: &AccountPrincipal, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [principal_mailbox_store_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)

# Called by

- [format_common_views_wlink_target_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [navigation_shortcut_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [serialize_navigation_shortcut_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)
- [common_views_message_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal.md)