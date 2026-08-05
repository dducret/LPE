---
type: Rust Method
title: with_parent_folder_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L150-L153
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`pub(crate) fn with_parent_folder_id(mut self, parent_folder_id: Option<u64>) -> Self`

# Called by

- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)