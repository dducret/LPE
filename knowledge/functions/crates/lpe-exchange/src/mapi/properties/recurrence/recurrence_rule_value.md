---
type: Rust Function
title: recurrence_rule_value
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L224-L228
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
---

# Signature

`fn recurrence_rule_value(parts: &[(String, String)], key: &str) -> Option<String>`

# Called by

- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)