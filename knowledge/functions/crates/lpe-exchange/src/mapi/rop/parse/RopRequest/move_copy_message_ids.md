---
type: Rust Method
title: move_copy_message_ids
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L833-L846
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn move_copy_message_ids(&self) -> Vec<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [calendar_same_folder_move_partial_completion](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion.md)
- [calendar_move_to_deleted_items_partial_completion](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion.md)
- [append_move_copy_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)