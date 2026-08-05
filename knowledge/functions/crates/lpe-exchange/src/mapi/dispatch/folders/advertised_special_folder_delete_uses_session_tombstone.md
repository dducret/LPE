---
type: Rust Function
title: advertised_special_folder_delete_uses_session_tombstone
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1000-L1002
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
---

# Signature

`pub(super) fn advertised_special_folder_delete_uses_session_tombstone(folder_id: u64) -> bool`

# Called by

- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)