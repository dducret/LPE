---
type: Rust Function
title: bounded_rule_mutation_from_row
resource: crates/lpe-exchange/src/mapi/dispatch/rules.rs#L194-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_sieve_from_json
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
---

# Signature

`pub(super) fn bounded_rule_mutation_from_row( row: &ModifyRulesRow, ) -> Result<BoundedRuleMutation, u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [bounded_rule_sieve_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_sieve_from_json.md)

# Called by

- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)