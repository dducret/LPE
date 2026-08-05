---
type: Rust Method
title: hierarchy_move_or_copy
resource: crates/lpe-exchange/src/mapi/notifications.rs#L168-L205
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_move_emits_a_source_parent_table_refresh
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
---

# Signature

`pub(crate) fn hierarchy_move_or_copy( event_mask: u16, parent_folder_id: u64, folder_id: u64, old_folder_id: u64, old_parent_folder_id: u64, ) -> Self`

# Called by

- [append_folder_move_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately.md)
- [hierarchy_move_emits_a_source_parent_table_refresh](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_move_emits_a_source_parent_table_refresh.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)