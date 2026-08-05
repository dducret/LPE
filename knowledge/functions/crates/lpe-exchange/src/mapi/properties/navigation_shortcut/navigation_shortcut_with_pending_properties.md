---
type: Rust Function
title: navigation_shortcut_with_pending_properties
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L55-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_with_pending_properties( message: &MapiNavigationShortcutMessage, account_id: Uuid, pending_properties: &HashMap<u32, MapiValue>, deleted_properties: &HashSet<u32>, ) -> MapiNavigationShortcutMessage`

# Calls

- [navigation_shortcut_properties_with_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending.md)
- [navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)