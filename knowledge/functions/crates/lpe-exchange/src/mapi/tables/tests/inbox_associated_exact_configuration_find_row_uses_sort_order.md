---
type: Rust Function
title: inbox_associated_exact_configuration_find_row_uses_sort_order
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L6306-L6348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_sort_snapshot
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16
---

# Signature

`fn inbox_associated_exact_configuration_find_row_uses_sort_order()`

# Calls

- [inbox_associated_sort_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_sort_snapshot.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [assert_response_contains_utf16](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16.md)