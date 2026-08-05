---
type: Rust Method
title: move_copy_want_asynchronous
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L860-L868
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
---

# Signature

`pub(in crate::mapi) fn move_copy_want_asynchronous(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_move_copy_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)