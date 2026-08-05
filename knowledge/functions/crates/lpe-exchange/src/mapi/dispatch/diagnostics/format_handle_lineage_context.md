---
type: Rust Function
title: format_handle_lineage_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1394-L1414
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn format_handle_lineage_context(object: Option<&MapiObject>) -> String`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)