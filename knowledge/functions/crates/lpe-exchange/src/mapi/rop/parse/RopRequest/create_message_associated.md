---
type: Rust Method
title: create_message_associated
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L89-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
---

# Signature

`pub(in crate::mapi) fn create_message_associated(&self) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_create_message_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)