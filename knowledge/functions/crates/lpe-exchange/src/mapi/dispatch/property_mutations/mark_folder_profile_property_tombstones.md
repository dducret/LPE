---
type: Rust Function
title: mark_folder_profile_property_tombstones
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L31-L57
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/is_lazy_folder_profile_property_tag
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set
---

# Signature

`fn mark_folder_profile_property_tombstones( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, object: Option<&MapiObject>, property_tags: &[u32], )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_lazy_folder_profile_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/is_lazy_folder_profile_property_tag.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [folder_profile_tombstones_are_handle_local_and_clear_on_set](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set.md)