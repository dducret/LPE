---
type: Rust Function
title: serialize_rule_row
resource: crates/lpe-exchange/src/mapi/tables/rules.rs#L22-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/tables/rules/rule_sequence
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule
---

# Signature

`pub(in crate::mapi) fn serialize_rule_row(rule: &MapiRule, columns: &[u32]) -> Vec<u8>`

# Calls

- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [rule_sequence](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/rule_sequence.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [rule_table_projects_canonical_sieve_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule.md)