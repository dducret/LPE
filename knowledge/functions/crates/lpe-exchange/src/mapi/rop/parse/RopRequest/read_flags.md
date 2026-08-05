---
type: Rust Method
title: read_flags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L231-L237
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
---

# Signature

`pub(in crate::mapi) fn read_flags(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_set_message_read_flag_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)