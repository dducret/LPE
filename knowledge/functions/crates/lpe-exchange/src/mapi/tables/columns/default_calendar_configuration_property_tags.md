---
type: Rust Function
title: default_calendar_configuration_property_tags
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L287-L296
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn default_calendar_configuration_property_tags() -> Vec<u32>`

# Calls

- [default_message_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)

# Called by

- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)