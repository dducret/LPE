---
type: Rust Function
title: hierarchy_row_parent_id
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L452-L463
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
---

# Signature

`pub(super) fn hierarchy_row_parent_id(row: &HierarchyRow<'_>, _mailboxes: &[JmapMailbox]) -> u64`

# Calls

- [mapi_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)