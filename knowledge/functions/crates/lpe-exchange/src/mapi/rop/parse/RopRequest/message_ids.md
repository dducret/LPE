---
type: Rust Method
title: message_ids
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L723-L740
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn message_ids(&self) -> Vec<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_set_read_flags_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)
- [append_delete_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)