---
type: Rust Method
title: modify_rules_rows
resource: crates/lpe-exchange/src/mapi/rop/property_rows.rs#L22-L28
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_count
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
---

# Signature

`pub(in crate::mapi) fn modify_rules_rows(&self) -> Result<Vec<ModifyRulesRow>>`

# Calls

- [modify_rules_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_count.md)
- [parse_modify_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_modify_rules_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)