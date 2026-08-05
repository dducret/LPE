---
type: Rust Method
title: move_copy_target_handle
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L986-L991
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn move_copy_target_handle(&self, input_handles: &[u32]) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_folder_move_copy_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [append_move_copy_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_copy_to_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)
- [append_copy_to_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)
- [append_copy_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)