---
type: Rust Function
title: stage_existing_navigation_shortcut_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L3-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) fn stage_existing_navigation_shortcut_property_values( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, values: Vec<(u32, MapiValue)>, ) -> Result<()>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)