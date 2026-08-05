---
type: Rust Function
title: append_register_notification_response
resource: crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions.rs#L42-L187
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_register_notification_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_notification_registration_context
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_common_views_inbox_notification_without_contents
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_notification_handoff_logged
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_notification_dispatch_response
---

# Signature

`pub(super) async fn append_register_notification_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, logon_id: u8, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [notification_registration_from_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [mapi_object_debug_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [format_handle_lineage_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_register_notification_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [record_last_inbox_notification_registration_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_notification_registration_context.md)
- [record_mapi_outlook_view_post_common_views_inbox_notification_without_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_common_views_inbox_notification_without_contents.md)
- [record_mapi_outlook_view_bootstrap_stall](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [mark_post_common_views_notification_handoff_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_notification_handoff_logged.md)

# Called by

- [append_notification_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_notification_dispatch_response.md)