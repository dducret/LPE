---
type: Rust Method
title: pending_collaboration_hierarchy_notification_requires_contents
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L82-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi) fn pending_collaboration_hierarchy_notification_requires_contents( &self, ) -> bool`

# Calls

- [folder_counts_modified_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event.md)
- [folder_counts_hierarchy_table_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event.md)
- [table_matches_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event.md)

# Called by

- [execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)