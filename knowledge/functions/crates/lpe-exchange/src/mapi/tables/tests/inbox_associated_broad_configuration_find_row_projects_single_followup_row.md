---
type: Rust Function
title: inbox_associated_broad_configuration_find_row_projects_single_followup_row
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L6229-L6313
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
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response
---

# Signature

`fn inbox_associated_broad_configuration_find_row_projects_single_followup_row()`

# Calls

- [inbox_associated_sort_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_sort_snapshot.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [assert_response_contains_utf16](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16.md)
- [rop_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response.md)