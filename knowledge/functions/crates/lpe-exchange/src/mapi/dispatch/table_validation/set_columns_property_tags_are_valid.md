---
type: Rust Function
title: set_columns_property_tags_are_valid
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L174-L182
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid_for_rule_table
---

# Signature

`fn set_columns_property_tags_are_valid(property_tags: &[u32]) -> bool`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)

# Called by

- [set_columns_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid.md)
- [set_columns_property_tags_are_valid_for_rule_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid_for_rule_table.md)