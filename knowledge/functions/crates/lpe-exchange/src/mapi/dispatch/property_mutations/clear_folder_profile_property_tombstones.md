---
type: Rust Function
title: clear_folder_profile_property_tombstones
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L8-L29
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/is_lazy_folder_profile_property_tag
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set
---

# Signature

`fn clear_folder_profile_property_tombstones( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, values: &[(u32, MapiValue)], )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_lazy_folder_profile_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/is_lazy_folder_profile_property_tag.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [folder_profile_tombstones_are_handle_local_and_clear_on_set](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set.md)