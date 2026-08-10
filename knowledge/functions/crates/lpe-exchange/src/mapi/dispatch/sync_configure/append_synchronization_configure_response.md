---
type: Rust Function
title: append_synchronization_configure_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_configure.rs#L8-L590
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_type
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_send_options
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_flags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_extra_flags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_are_supported
  - functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_kind
  - functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/hierarchy_checkpoint_status
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/scope/folder_is_in_hierarchy_sync_scope
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_sync_objects
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/changed_special_ids_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/changed_sync_mailboxes
  - functions/crates/lpe-exchange/src/mapi/sync/changed_sync_emails
  - functions/crates/lpe-exchange/src/mapi/sync/changed_special_sync_objects
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content
  - functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_message_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_versions
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_configure_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder
---

# Signature

`pub(super) async fn append_synchronization_configure_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, content_sync_configure_observed: &mut bool, ) -> SyncConfigureFlow`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [sync_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_type.md)
- [sync_send_options](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_send_options.md)
- [sync_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_flags.md)
- [sync_extra_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_extra_flags.md)
- [sync_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_property_tags.md)
- [property_tags_are_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_are_supported.md)
- [sync_checkpoint_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_kind.md)
- [sync_checkpoint_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id.md)
- [log_calendar_identity_chain](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain.md)
- [debug_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [debug_container_class_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)
- [hierarchy_checkpoint_status](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/hierarchy_checkpoint_status.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fetch_mapi_sync_changes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [sync_state_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for_excluding_deleted.md)
- [sync_emails_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [folder_is_in_hierarchy_sync_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/folder_is_in_hierarchy_sync_scope.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [log_calendar_special_sync_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)
- [log_special_sync_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_sync_objects.md)
- [changed_special_ids_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/changed_special_ids_for_folder.md)
- [changed_sync_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_sync_mailboxes.md)
- [changed_sync_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_sync_emails.md)
- [changed_special_sync_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_special_sync_objects.md)
- [sync_attachment_facts_for_with_embedded_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content.md)
- [normal_message_sync_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [mapi_message_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_message_ids_for_deleted_changes.md)
- [mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)
- [deleted_special_object_ids_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder.md)
- [folder_versions](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_versions.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)
- [try_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [folder_local_commit_time_max](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [initial_sync_state_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [log_hierarchy_transfer_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug.md)
- [log_fai_content_sync_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_configure_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [record_content_sync_configure_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder.md)