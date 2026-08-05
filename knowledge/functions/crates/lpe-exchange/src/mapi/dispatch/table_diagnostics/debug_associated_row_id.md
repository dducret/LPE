---
type: Rust Function
title: debug_associated_row_id
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L526-L531
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
---

# Signature

`pub(super) fn debug_associated_row_id(message: &DebugAssociatedTableRow) -> u64`

# Called by

- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_folder_local_default_view_fai_visibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract.md)
- [sort_debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)