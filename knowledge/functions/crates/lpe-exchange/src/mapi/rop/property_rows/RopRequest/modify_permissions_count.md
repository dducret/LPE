---
type: Rust Method
title: modify_permissions_count
resource: crates/lpe-exchange/src/mapi/rop/property_rows.rs#L6-L12
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows
---

# Signature

`pub(in crate::mapi) fn modify_permissions_count(&self) -> Option<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [modify_permissions_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows.md)