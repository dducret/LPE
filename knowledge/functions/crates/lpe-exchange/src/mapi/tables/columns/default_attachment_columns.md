---
type: Rust Function
title: default_attachment_columns
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L129-L148
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn default_attachment_columns() -> Vec<u32>`

# Called by

- [get_properties_specific_candidate_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [rop_get_properties_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)