---
type: Rust Function
title: reminder_search_content_rows
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L29-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(super) fn reminder_search_content_rows<'a>( snapshot: &'a MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, ) -> Vec<SearchContentRow<'a>>`

# Calls

- [reminder_tasks](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks.md)
- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [reminder_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages.md)
- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)