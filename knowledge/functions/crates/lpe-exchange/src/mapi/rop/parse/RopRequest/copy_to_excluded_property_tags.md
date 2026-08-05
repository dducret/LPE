---
type: Rust Method
title: copy_to_excluded_property_tags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1007-L1022
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
---

# Signature

`pub(in crate::mapi) fn copy_to_excluded_property_tags(&self) -> Vec<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_copy_to_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)