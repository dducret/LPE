---
type: Rust Function
title: advertised_special_folder_delete_is_noop
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1004-L1012
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
---

# Signature

`pub(super) fn advertised_special_folder_delete_is_noop(folder_id: u64) -> bool`

# Called by

- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)