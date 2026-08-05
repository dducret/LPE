---
type: Rust Function
title: append_release_response
resource: crates/lpe-exchange/src/mapi/dispatch/release.rs#L44-L538
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_related_release_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_request_metrics
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_stage
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_post_fai_hierarchy_release_without_inbox_contents_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/abandon_event_attachment_transaction
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/session/release_handle_slot
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_related_release_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_fai_handoff_without_contents
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_handoff_logged
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_fai_hierarchy_without_contents
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_dispatch_response
---

# Signature

`pub(super) async fn append_release_response<S: ExchangeStore>( _store: &S, principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, same_execute_released_handles: &mut HashSet<u32>, post_hierarchy_release_events: &mut Vec<PostHierarchyReleaseDebugEvent>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [mapi_object_debug_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [format_inbox_related_release_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_related_release_context.md)
- [format_visible_inbox_release_request_metrics](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_request_metrics.md)
- [format_visible_inbox_release_stage](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_stage.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [format_inbox_post_fai_handoff_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context.md)
- [format_live_handle_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary.md)
- [format_post_fai_hierarchy_release_without_inbox_contents_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_post_fai_hierarchy_release_without_inbox_contents_context.md)
- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [format_optional_debug_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle.md)
- [record_logoff_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion.md)
- [abandon_event_attachment_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/abandon_event_attachment_transaction.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [release_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/release_handle_slot.md)
- [record_last_inbox_related_release_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_related_release_context.md)
- [record_last_table_release_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_mapi_outlook_view_inbox_fai_handoff_without_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_fai_handoff_without_contents.md)
- [record_mapi_outlook_view_bootstrap_stall](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall.md)
- [mark_post_inbox_fai_handoff_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_handoff_logged.md)
- [record_mapi_outlook_view_post_fai_hierarchy_without_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_fai_hierarchy_without_contents.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)

# Called by

- [append_release_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_dispatch_response.md)