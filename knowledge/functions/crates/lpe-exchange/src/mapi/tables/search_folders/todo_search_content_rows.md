---
type: Rust Function
title: todo_search_content_rows
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L9-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(super) fn todo_search_content_rows<'a>( snapshot: &'a MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, ) -> Vec<SearchContentRow<'a>>`

# Calls

- [todo_search_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages.md)
- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [todo_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results.md)
- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)