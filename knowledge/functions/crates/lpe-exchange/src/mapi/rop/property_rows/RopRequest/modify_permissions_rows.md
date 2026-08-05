---
type: Rust Method
title: modify_permissions_rows
resource: crates/lpe-exchange/src/mapi/rop/property_rows.rs#L30-L36
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_count
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
---

# Signature

`pub(in crate::mapi) fn modify_permissions_rows(&self) -> Result<Vec<ModifyRulesRow>>`

# Calls

- [modify_permissions_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_count.md)
- [parse_modify_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_modify_permissions_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)