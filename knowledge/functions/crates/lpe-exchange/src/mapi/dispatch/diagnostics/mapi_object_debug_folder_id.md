---
type: Rust Function
title: mapi_object_debug_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1463-L1468
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn mapi_object_debug_folder_id(object: Option<&MapiObject>) -> String`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)