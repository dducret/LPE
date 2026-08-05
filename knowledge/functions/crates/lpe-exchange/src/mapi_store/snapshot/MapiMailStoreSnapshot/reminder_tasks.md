---
type: Rust Method
title: reminder_tasks
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1081-L1097
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
---

# Signature

`pub(crate) fn reminder_tasks(&self) -> Vec<&MapiTask>`

# Calls

- [search_folder_definition_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role.md)

# Called by

- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [reminder_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows.md)
- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)