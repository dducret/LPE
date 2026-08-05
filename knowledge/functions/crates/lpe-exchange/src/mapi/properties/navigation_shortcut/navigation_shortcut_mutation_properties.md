---
type: Rust Function
title: navigation_shortcut_mutation_properties
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L25-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_mutation_properties( message: &MapiNavigationShortcutMessage, account_id: Uuid, ) -> HashMap<u32, MapiValue>`

# Calls

- [navigation_shortcut_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)

# Called by

- [navigation_shortcut_properties_with_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending.md)