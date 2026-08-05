---
type: Rust Function
title: rule_table_object
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L206-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
---

# Signature

`pub(super) fn rule_table_object(folder_id: u64) -> MapiObject`

# Calls

- [default_rule_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns.md)

# Called by

- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)