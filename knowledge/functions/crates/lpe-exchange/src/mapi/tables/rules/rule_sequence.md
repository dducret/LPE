---
type: Rust Function
title: rule_sequence
resource: crates/lpe-exchange/src/mapi/tables/rules.rs#L51-L55
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row
---

# Signature

`fn rule_sequence(rule_id: u64) -> u32`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [serialize_rule_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row.md)