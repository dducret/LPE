---
type: Rust Function
title: restriction_matches_task
resource: crates/lpe-exchange/src/mapi/properties.rs#L337-L344
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_matches
---

# Signature

`pub(in crate::mapi) fn restriction_matches_task( restriction: Option<&MapiRestriction>, task: &ClientTask, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [task_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value.md)

# Called by

- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [todo_search_content_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows.md)
- [reminder_search_content_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows.md)
- [search_content_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_matches.md)