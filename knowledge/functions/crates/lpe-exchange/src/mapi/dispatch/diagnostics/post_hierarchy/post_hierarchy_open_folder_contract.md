---
type: Rust Function
title: post_hierarchy_open_folder_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L176-L187
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn post_hierarchy_open_folder_contract( folder_id: u64, result: &str, ) -> String`

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)