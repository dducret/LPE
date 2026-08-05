---
type: Rust Method
title: remember_table_notification_eligibility
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L9-L22
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
---

# Signature

`pub(in crate::mapi) fn remember_table_notification_eligibility( &mut self, handle: u32, logon_id: u8, notifications_enabled: bool, )`

# Calls

- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_open_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)