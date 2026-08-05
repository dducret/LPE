---
type: Rust Function
title: set_columns_property_tags_are_valid_for_rule_table
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L184-L191
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table
---

# Signature

`fn set_columns_property_tags_are_valid_for_rule_table(property_tags: &[u32]) -> bool`

# Calls

- [set_columns_property_tags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid.md)

# Called by

- [set_columns_request_is_valid_for_rule_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table.md)