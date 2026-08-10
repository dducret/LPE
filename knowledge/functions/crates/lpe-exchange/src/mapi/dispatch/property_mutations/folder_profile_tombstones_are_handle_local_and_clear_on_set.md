---
type: Rust Function
title: folder_profile_tombstones_are_handle_local_and_clear_on_set
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L699-L783
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones
---

# Signature

`fn folder_profile_tombstones_are_handle_local_and_clear_on_set()`

# Calls

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [mark_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones.md)
- [clear_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones.md)