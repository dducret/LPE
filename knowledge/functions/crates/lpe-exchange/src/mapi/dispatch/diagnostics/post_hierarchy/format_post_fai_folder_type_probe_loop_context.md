---
type: Rust Function
title: format_post_fai_folder_type_probe_loop_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L573-L597
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/post_fai_folder_type_probe_loop_context_requires_reopen_and_repeated_probes
---

# Signature

`pub(in crate::mapi::dispatch) fn format_post_fai_folder_type_probe_loop_context( state: &PostHierarchyActionState, ) -> Option<String>`

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [post_fai_folder_type_probe_loop_context_requires_reopen_and_repeated_probes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/post_fai_folder_type_probe_loop_context_requires_reopen_and_repeated_probes.md)