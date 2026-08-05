---
type: Rust Method
title: message_status_flags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L773-L779
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
---

# Signature

`pub(in crate::mapi) fn message_status_flags(&self) -> u32`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_message_status_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)