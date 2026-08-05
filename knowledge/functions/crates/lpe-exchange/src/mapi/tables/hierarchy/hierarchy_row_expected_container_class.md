---
type: Rust Function
title: hierarchy_row_expected_container_class
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L400-L411
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/debug_expected_container_class
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
---

# Signature

`pub(super) fn hierarchy_row_expected_container_class<'a>( row: &'a HierarchyRow<'a>, ) -> Option<&'a str>`

# Calls

- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [debug_expected_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/debug_expected_container_class.md)
- [folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)