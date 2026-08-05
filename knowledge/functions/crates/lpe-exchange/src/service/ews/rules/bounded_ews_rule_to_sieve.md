---
type: Rust Function
title: bounded_ews_rule_to_sieve
resource: crates/lpe-exchange/src/service/ews/rules.rs#L158-L215
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules
---

# Signature

`pub(in crate::service) fn bounded_ews_rule_to_sieve(rule: &str) -> Result<(String, bool, String)>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)

# Called by

- [update_inbox_rules](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules.md)