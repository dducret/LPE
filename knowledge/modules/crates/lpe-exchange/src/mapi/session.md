---
type: Rust Module
title: session
resource: crates/lpe-exchange/src/mapi/session.rs#L1-L1532
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-dispatch
  - external/super-notifications
  - external/super-outlook-startup
  - external/super-properties
  - external/super-rop
  - external/super-store-adapter
  - external/super-sync
  - external/super-transport
  - external/super
  - external/crate-mapi-wire-ropid
  - external/crate-mapi-store-mapiassociatedconfigmessage
  - external/lpe-storage-attachmentuploadinput-jmapemail-mapicontactimportedidentity-mapieventattachmentchanges-mapieventimportedidentity-searchfolderdefinition
  - external/pub-crate-use-lifecycle-begin-active-session-request-for-test
  - external/pub-in-crate-mapi-use-lifecycle
  - external/pub-crate-use-lifecycle-create-rpc-emsmdb-context-execute-rpc-emsmdb-rops
  - external/pub-in-crate-mapi-use-named-properties
  - external/pub-in-crate-mapi-use-types
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [session_debug_context_or_none](../../../../../functions/crates/lpe-exchange/src/mapi/session/session_debug_context_or_none.md)
- [record_logon_identity](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logon_identity.md)
- [record_transport_request](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request.md)
- [record_completed_sync_checkpoint](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_sync_checkpoint.md)
- [record_opened_folder](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder.md)
- [record_message_handle_generation](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_handle_generation.md)
- [message_save_generation](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_save_generation.md)
- [message_handle_generation](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_handle_generation.md)
- [record_message_saved](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_saved.md)
- [record_inbox_open_folder_probe](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_open_folder_probe.md)
- [record_inbox_folder_type_getprops_probe](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_folder_type_getprops_probe.md)
- [record_inbox_normal_contents_table](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table.md)
- [record_inbox_normal_contents_table_setcolumns](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_setcolumns.md)
- [record_inbox_normal_contents_table_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_rows.md)
- [record_inbox_normal_contents_table_find_row](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_find_row.md)
- [record_inbox_normal_contents_table_query_position](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_position.md)
- [record_calendar_normal_contents_table_query_position](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_position.md)
- [record_calendar_normal_contents_table_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_rows.md)
- [record_default_view_normal_contents_table_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_normal_contents_table_query_rows.md)
- [record_default_view_folder_opened](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_folder_opened.md)
- [record_post_calendar_query_position_named_property_probe](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_calendar_query_position_named_property_probe.md)
- [record_inbox_associated_contents_table](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_associated_broad_findrow](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_broad_findrow.md)
- [record_inbox_associated_exact_findrow](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow.md)
- [record_inbox_associated_findrow_returned_content](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content.md)
- [record_inbox_associated_query_rows_returned_non_empty](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_returned_non_empty.md)
- [record_inbox_associated_non_empty_query_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_non_empty_query_context.md)
- [record_inbox_associated_query_rows_reached_end](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_reached_end.md)
- [record_receive_folder_verification_passed](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [record_inbox_associated_config_open](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_open.md)
- [record_inbox_associated_config_stream_open](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_open.md)
- [record_inbox_associated_config_stream_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_handle.md)
- [record_inbox_rule_organizer_stream_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_handle.md)
- [is_inbox_associated_config_stream_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle.md)
- [is_inbox_rule_organizer_stream_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_rule_organizer_stream_handle.md)
- [record_inbox_associated_config_stream_read](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_read.md)
- [record_inbox_rule_organizer_stream_read](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_read.md)
- [record_last_inbox_open_folder_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_open_folder_context.md)
- [record_last_inbox_contents_table_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_contents_table_context.md)
- [record_last_inbox_associated_query_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context.md)
- [record_last_inbox_associated_find_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_find_context.md)
- [record_last_common_views_inbox_shortcut_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_common_views_inbox_shortcut_context.md)
- [record_last_inbox_notification_registration_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_notification_registration_context.md)
- [record_last_inbox_hierarchy_table_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_table_context.md)
- [record_last_inbox_hierarchy_query_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_query_context.md)
- [record_last_hierarchy_table_query_position_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_hierarchy_table_query_position_context.md)
- [record_last_inbox_related_release_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_related_release_context.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)
- [record_post_hierarchy_submit_attempt_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context.md)
- [record_last_inbox_folder_type_getprops_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_folder_type_getprops_context.md)
- [record_last_successful_execute_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_successful_execute_context.md)
- [record_last_table_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_context.md)
- [record_last_table_query_rows_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_query_rows_context.md)
- [record_last_table_release_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context.md)
- [abandoned_after_inbox_fai_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/abandoned_after_inbox_fai_query_rows.md)
- [record_first_inbox_loop_transition_context](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_first_inbox_loop_transition_context.md)
- [mark_inbox_loop_transition_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_inbox_loop_transition_logged.md)
- [mark_post_inbox_fai_handoff_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_handoff_logged.md)
- [mark_post_common_views_handoff_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_handoff_logged.md)
- [mark_post_common_views_notification_handoff_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_notification_handoff_logged.md)
- [mark_post_common_views_inbox_open_loop_metric_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_inbox_open_loop_metric_logged.md)
- [mark_post_inbox_fai_reopen_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_reopen_logged.md)
- [mark_post_inbox_fai_folder_type_probe_loop_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_folder_type_probe_loop_logged.md)
- [mark_post_rule_organizer_stream_reopen_logged](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_rule_organizer_stream_reopen_logged.md)
- [record_recent_probe_action](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [record_outlook_view_failure_trace_event](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_default_view_advertised](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised.md)
- [record_default_view_opened](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_opened.md)
- [format_default_view_advertisement_state](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/format_default_view_advertisement_state.md)
- [default_view_advertisement_state](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state.md)
- [default_view_advertisement_state_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state_for_folder.md)
- [default_view_advertisement_summary](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary.md)
- [default_view_folder_open_match_state](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_folder_open_match_state.md)
- [advertised_default_view_pending_open](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open.md)
- [record_outlook_stream_batch_observed](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_stream_batch_observed.md)
- [record_post_hierarchy_request_contract](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [record_post_hierarchy_getprops_contract](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_getprops_contract.md)
- [record_post_hierarchy_setprops_contract](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_setprops_contract.md)
- [record_special_folder_alias](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias.md)
- [replace_special_folder_aliases](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/replace_special_folder_aliases.md)
- [remember_search_folder_definition](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/remember_search_folder_definition.md)
- [search_folder_definition](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition.md)
- [forget_search_folder_definition](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/forget_search_folder_definition.md)
- [search_folder_definition_was_deleted](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted.md)
- [resolve_special_folder_alias](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [record_deleted_advertised_special_folder](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_deleted_advertised_special_folder.md)
- [advertised_special_folder_was_deleted](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_special_folder_was_deleted.md)
- [allocate_output_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [allocate_output_handle_avoiding](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle_avoiding.md)
- [hierarchy_sync_completed](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [record_completed_hierarchy_sync](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_hierarchy_sync.md)
- [record_content_sync_configure](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure.md)
- [record_content_sync_configure_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder.md)
- [record_logoff_after_hierarchy_completion](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion.md)
- [record_execute_after_hierarchy_completion](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion.md)
- [execute_has_create_save_batch](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/execute_has_create_save_batch.md)
- [record_visible_inbox_open_create_save_batch](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch.md)
- [record_post_visible_inbox_release_create_save_batch](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch.md)
- [folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiObject/folder_id.md)
- [input_object](../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [input_object_mut](../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [synchronization_context_state](../../../../../functions/crates/lpe-exchange/src/mapi/session/synchronization_context_state.md)
- [input_handle](../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [set_handle_slot](../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [release_handle_slot](../../../../../functions/crates/lpe-exchange/src/mapi/session/release_handle_slot.md)
- [response_handle_table](../../../../../functions/crates/lpe-exchange/src/mapi/session/response_handle_table.md)
- [response_handle_table_with_released_handle_sentinel](../../../../../functions/crates/lpe-exchange/src/mapi/session/response_handle_table_with_released_handle_sentinel.md)
- [reset_table_state](../../../../../functions/crates/lpe-exchange/src/mapi/session/reset_table_state.md)
- [read_handle_table](../../../../../functions/crates/lpe-exchange/src/mapi/session/read_handle_table.md)

# Imports

- `super::dispatch::*`
- `super::notifications::*`
- `super::outlook_startup::*`
- `super::properties::*`
- `super::rop::*`
- `super::store_adapter::*`
- `super::sync::*`
- `super::transport::*`
- `super::*`
- `crate::mapi::wire::RopId`
- `crate::mapi_store::MapiAssociatedConfigMessage`
- `lpe_storage::{
    AttachmentUploadInput, JmapEmail, MapiContactImportedIdentity, MapiEventAttachmentChanges,
    MapiEventImportedIdentity, SearchFolderDefinition,
}`
- `pub(crate) use lifecycle::begin_active_session_request_for_test`
- `pub(in crate::mapi) use lifecycle::*`
- `pub(crate) use lifecycle::{create_rpc_emsmdb_context, execute_rpc_emsmdb_rops}`
- `pub(in crate::mapi) use named_properties::*`
- `pub(in crate::mapi) use types::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)