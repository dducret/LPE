---
type: Rust Method
title: delete_messages_notify_non_read
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L752-L760
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
---

# Signature

`pub(in crate::mapi) fn delete_messages_notify_non_read(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_delete_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)