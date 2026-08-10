---
type: Rust Function
title: folder_handle_for_id
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L306-L346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_containing_folder_response_handle
---

# Signature

`fn folder_handle_for_id( session: &MapiSession, handle_slots: &[u32], preferred_handle: u32, folder_id: u64, ) -> Option<u32>`

# Called by

- [restore_save_changes_containing_folder_response_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_containing_folder_response_handle.md)