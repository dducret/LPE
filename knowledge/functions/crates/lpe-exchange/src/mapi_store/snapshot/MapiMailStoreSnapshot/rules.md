---
type: Rust Method
title: rules
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1231-L1233
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule
---

# Signature

`pub(crate) fn rules(&self) -> &[MapiRule]`

# Called by

- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [rule_table_projects_canonical_sieve_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule.md)