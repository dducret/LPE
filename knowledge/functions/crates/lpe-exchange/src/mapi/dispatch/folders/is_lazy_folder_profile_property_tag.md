---
type: Rust Function
title: is_lazy_folder_profile_property_tag
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1048-L1053
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones
---

# Signature

`pub(super) fn is_lazy_folder_profile_property_tag(property_tag: u32) -> bool`

# Called by

- [clear_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones.md)
- [mark_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones.md)