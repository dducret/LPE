---
type: Rust Function
title: default_view_entry_id_target_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L309-L326
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn default_view_entry_id_target_for_debug( entry_id: &[u8], ) -> Option<(u64, u64)>`

# Calls

- [global_counter_from_globcnt](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [default_view_entry_id_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug.md)
- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)