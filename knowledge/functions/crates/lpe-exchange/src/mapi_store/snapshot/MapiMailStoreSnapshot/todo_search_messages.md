---
type: Rust Method
title: todo_search_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1008-L1029
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_followup_mail_into_todo_search_results
---

# Signature

`pub(crate) fn todo_search_messages(&self) -> Vec<&MapiMessage>`

# Calls

- [search_folder_definition_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role.md)

# Called by

- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [todo_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows.md)
- [todo_search_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_message_for_id.md)
- [snapshot_projects_followup_mail_into_todo_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_followup_mail_into_todo_search_results.md)