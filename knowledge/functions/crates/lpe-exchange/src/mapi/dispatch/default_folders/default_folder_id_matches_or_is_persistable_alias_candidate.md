---
type: Rust Function
title: default_folder_id_matches_or_is_persistable_alias_candidate
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L140-L150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
---

# Signature

`fn default_folder_id_matches_or_is_persistable_alias_candidate( folder_id: u64, expected_folder_id: u64, ) -> bool`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)