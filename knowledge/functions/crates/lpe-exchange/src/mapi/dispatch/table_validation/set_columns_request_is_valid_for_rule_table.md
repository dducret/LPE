---
type: Rust Function
title: set_columns_request_is_valid_for_rule_table
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L166-L172
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/table_async_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid_for_rule_table
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
---

# Signature

`pub(in crate::mapi::dispatch) fn set_columns_request_is_valid_for_rule_table( request: &RopRequest, ) -> bool`

# Calls

- [table_async_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/table_async_flags_are_valid.md)
- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [set_columns_property_tags_are_valid_for_rule_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid_for_rule_table.md)

# Called by

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)