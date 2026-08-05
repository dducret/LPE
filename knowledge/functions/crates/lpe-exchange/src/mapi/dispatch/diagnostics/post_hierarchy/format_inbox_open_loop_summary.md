---
type: Rust Function
title: format_inbox_open_loop_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L474-L525
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_open_loop_summary_requires_repeated_probe_without_contents_table
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_open_loop_summary( state: &PostHierarchyActionState, ) -> Option<String>`

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [inbox_open_loop_summary_requires_repeated_probe_without_contents_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_open_loop_summary_requires_repeated_probe_without_contents_table.md)