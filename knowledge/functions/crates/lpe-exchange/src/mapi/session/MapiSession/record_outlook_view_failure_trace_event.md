---
type: Rust Method
title: record_outlook_view_failure_trace_event
resource: crates/lpe-exchange/src/mapi/session.rs#L653-L667
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_stream_batch_observation
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/record_visible_inbox_message_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_folder_opened
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_calendar_query_position_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_hierarchy_table_query_position_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch
---

# Signature

`pub(in crate::mapi) fn record_outlook_view_failure_trace_event(&mut self, event: String)`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [log_message_getprops_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug.md)
- [record_outlook_umolk_getprops_materialization](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)
- [record_execute_stream_batch_observation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_stream_batch_observation.md)
- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [record_visible_inbox_message_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/record_visible_inbox_message_open.md)
- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [record_outlook_umolk_named_property_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe.md)
- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [append_commit_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [record_default_view_folder_opened](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_folder_opened.md)
- [record_post_calendar_query_position_named_property_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_calendar_query_position_named_property_probe.md)
- [record_last_hierarchy_table_query_position_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_hierarchy_table_query_position_context.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)
- [record_post_hierarchy_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context.md)
- [record_default_view_advertised](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised.md)
- [record_visible_inbox_open_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch.md)
- [record_post_visible_inbox_release_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch.md)