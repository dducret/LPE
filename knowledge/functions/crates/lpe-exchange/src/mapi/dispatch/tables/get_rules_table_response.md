---
type: Rust Function
title: get_rules_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1329-L1331
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/rules/rop_get_rules_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
---

# Signature

`pub(super) fn get_rules_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/rop_get_rules_table_response.md)

# Called by

- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)