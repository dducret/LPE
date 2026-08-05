---
type: Rust Function
title: inbox_post_fai_reopen_stall_observed
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L564-L571
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn inbox_post_fai_reopen_stall_observed( state: &PostHierarchyActionState, ) -> bool`

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)