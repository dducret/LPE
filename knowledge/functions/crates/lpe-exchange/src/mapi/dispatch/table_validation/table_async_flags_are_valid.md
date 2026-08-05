---
type: Rust Function
title: table_async_flags_are_valid
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L153-L158
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table
---

# Signature

`pub(in crate::mapi::dispatch) fn table_async_flags_are_valid(request: &RopRequest) -> bool`

# Called by

- [append_restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [set_columns_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid.md)
- [set_columns_request_is_valid_for_rule_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table.md)