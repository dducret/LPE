---
type: Rust Method
title: row_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L164-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
---

# Signature

`pub(in crate::mapi) fn row_id(&self) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_read_recipients_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)