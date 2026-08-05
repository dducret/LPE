---
type: Rust Method
title: modify_rules_count
resource: crates/lpe-exchange/src/mapi/rop/property_rows.rs#L14-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows
---

# Signature

`pub(in crate::mapi) fn modify_rules_count(&self) -> Option<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [modify_rules_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows.md)