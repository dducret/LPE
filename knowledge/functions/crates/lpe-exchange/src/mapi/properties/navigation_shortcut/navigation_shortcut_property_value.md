---
type: Rust Function
title: navigation_shortcut_property_value
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L3-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_mutation_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_property_value( message: &MapiNavigationShortcutMessage, account_id: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)

# Called by

- [format_common_views_wlink_target_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [restriction_matches_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut.md)
- [navigation_shortcut_mutation_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_mutation_properties.md)
- [associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys.md)
- [serialize_navigation_shortcut_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)