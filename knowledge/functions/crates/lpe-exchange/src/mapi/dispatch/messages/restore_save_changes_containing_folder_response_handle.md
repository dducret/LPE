---
type: Rust Function
title: restore_save_changes_containing_folder_response_handle
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L245-L274
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/folder_handle_for_id
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_response_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_associated_message_restores_containing_folder_response_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot
---

# Signature

`pub(super) fn restore_save_changes_containing_folder_response_handle( session: &MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, folder_id: u64, ) -> Option<u32>`

# Calls

- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [folder_handle_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/folder_handle_for_id.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)

# Called by

- [restore_save_changes_response_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_response_handle.md)
- [save_changes_associated_message_restores_containing_folder_response_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_associated_message_restores_containing_folder_response_handle_slot.md)
- [save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot.md)