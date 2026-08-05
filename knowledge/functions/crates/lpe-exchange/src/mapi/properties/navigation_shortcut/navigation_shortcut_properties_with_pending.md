---
type: Rust Function
title: navigation_shortcut_properties_with_pending
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L93-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_mutation_properties
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_properties_with_pending( message: &MapiNavigationShortcutMessage, account_id: Uuid, pending_properties: &HashMap<u32, MapiValue>, deleted_properties: &HashSet<u32>, ) -> HashMap<u32, MapiValue>`

# Calls

- [navigation_shortcut_mutation_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_mutation_properties.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [navigation_shortcut_with_pending_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties.md)