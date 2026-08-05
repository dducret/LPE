---
type: Rust Function
title: expected_special_folder_parent_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L423-L428
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract
---

# Signature

`pub(in crate::mapi::dispatch) fn expected_special_folder_parent_id(folder_id: u64) -> u64`

# Called by

- [default_folder_hierarchy_projection_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug.md)
- [log_special_folder_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract.md)