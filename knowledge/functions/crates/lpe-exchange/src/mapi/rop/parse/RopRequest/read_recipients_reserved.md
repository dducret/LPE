---
type: Rust Method
title: read_recipients_reserved
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L169-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
---

# Signature

`pub(in crate::mapi) fn read_recipients_reserved(&self) -> Option<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_read_recipients_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)