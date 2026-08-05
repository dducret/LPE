---
type: Rust Function
title: post_hierarchy_get_receive_folder_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L189-L198
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn post_hierarchy_get_receive_folder_contract( message_class: &str, folder_id: u64, ) -> String`

# Called by

- [append_get_receive_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)