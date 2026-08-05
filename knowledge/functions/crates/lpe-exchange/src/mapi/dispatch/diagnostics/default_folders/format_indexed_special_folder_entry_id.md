---
type: Rust Function
title: format_indexed_special_folder_entry_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L493-L518
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/indexed_special_folder_entry_ids_for_debug
---

# Signature

`fn format_indexed_special_folder_entry_id( index: usize, bytes: &[u8], expected_folder_id: u64, ) -> String`

# Calls

- [post_hierarchy_probe_folder_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name.md)

# Called by

- [indexed_special_folder_entry_ids_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/indexed_special_folder_entry_ids_for_debug.md)