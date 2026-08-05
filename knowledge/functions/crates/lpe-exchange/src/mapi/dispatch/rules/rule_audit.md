---
type: Rust Function
title: rule_audit
resource: crates/lpe-exchange/src/mapi/dispatch/rules.rs#L349-L359
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
---

# Signature

`pub(super) fn rule_audit( principal: &AccountPrincipal, action: &str, subject: &str, ) -> AuditEntryInput`

# Called by

- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)