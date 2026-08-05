---
type: Rust Function
title: post_hierarchy_probe_folder_name
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L199-L241
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/format_indexed_special_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
---

# Signature

`pub(in crate::mapi) fn post_hierarchy_probe_folder_name(folder_id: u64) -> &'static str`

# Called by

- [debug_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [default_folder_entry_id_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)
- [format_indexed_special_folder_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/format_indexed_special_folder_entry_id.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)