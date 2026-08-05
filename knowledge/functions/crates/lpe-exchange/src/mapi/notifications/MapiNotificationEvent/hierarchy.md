---
type: Rust Method
title: hierarchy
resource: crates/lpe-exchange/src/mapi/notifications.rs#L72-L95
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/source_hierarchy_table_event
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
---

# Signature

`pub(in crate::mapi) fn hierarchy(folder_id: u64, changed_folder_id: Option<u64>) -> Self`

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [append_folder_move_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [source_hierarchy_table_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/source_hierarchy_table_event.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)