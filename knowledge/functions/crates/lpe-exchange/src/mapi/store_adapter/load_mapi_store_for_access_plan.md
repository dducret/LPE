---
type: Rust Function
title: load_mapi_store_for_access_plan
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L95-L898
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_full_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_step
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_identity_resolution
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/merge_requested_mailboxes
  - functions/crates/lpe-exchange/src/mapi/store_adapter/requested_identity_has_backing_row
  - functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mailbox_id_for_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/query_mapi_content_table_ids
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_navigation_shortcuts
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_contact_sync_versions
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_collaboration_resolution
  - functions/crates/lpe-exchange/src/mapi/store_adapter/deduplicate_mapi_identity_requests
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_identity_request_summary
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/store_adapter/calendar_event_versions_required
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_permissions
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_mailbox_content_commit_times
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_summary
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_identities
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_sync_versions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_event_versions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_notes_and_journal
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_conversation_actions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcut_identities
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_named_property_mappings
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_config_identity_ids
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_reminders
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_calendar_attachments
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_mailbox_content_commit_times
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_identity_codec
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
---

# Signature

`pub(in crate::mapi) async fn load_mapi_store_for_access_plan<S>( store: &S, account_id: Uuid, identity_scope: &MapiIdentityScope, request_identity_scope: &crate::mapi::identity::MapiRequestIdentityScope, plan: &MapiAccessPlan, full_message_limit: u64, ) -> Result<MapiMailStoreSnapshot> where S: ExchangeStore,`

# Calls

- [log_mapi_store_full_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_full_snapshot.md)
- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [context](../../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [finalize_mapi_store_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)
- [log_mapi_store_load_step](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_step.md)
- [fetch_mapi_identities_by_object_ids](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)
- [log_mapi_requested_identity_resolution](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_identity_resolution.md)
- [fetch_mapi_associated_configs](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs.md)
- [fetch_mapi_named_properties](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties.md)
- [merge_requested_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/merge_requested_mailboxes.md)
- [requested_identity_has_backing_row](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/requested_identity_has_backing_row.md)
- [forget_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)
- [remember_mapi_identity_with_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [mailbox_id_for_mapi_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mailbox_id_for_mapi_folder_id.md)
- [query_mapi_content_table_ids](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/query_mapi_content_table_ids.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_or_allocate_mapi_identities](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [fetch_message_attachments](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments.md)
- [collaboration_folder_identity_requests](../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests.md)
- [requires_snapshot_backed_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents.md)
- [fetch_mapi_navigation_shortcuts](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_navigation_shortcuts.md)
- [fetch_contact_sync_versions](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_contact_sync_versions.md)
- [fetch_accessible_tasks_in_collection](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection.md)
- [fetch_accessible_tasks_by_ids](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids.md)
- [log_mapi_requested_collaboration_resolution](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_collaboration_resolution.md)
- [deduplicate_mapi_identity_requests](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/deduplicate_mapi_identity_requests.md)
- [log_mapi_identity_request_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_identity_request_summary.md)
- [modeled_virtual_associated_config_message_for_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id.md)
- [calendar_event_versions_required](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/calendar_event_versions_required.md)
- [fetch_mapi_folder_permissions](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_permissions.md)
- [fetch_mapi_mailbox_content_commit_times](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_mailbox_content_commit_times.md)
- [log_mapi_store_load_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_summary.md)
- [new_with_scoped_calendar_identities](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities.md)
- [with_contact_identities](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_identities.md)
- [with_contact_sync_versions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_sync_versions.md)
- [with_event_versions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_event_versions.md)
- [with_notes_and_journal](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_notes_and_journal.md)
- [with_search_folder_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [with_conversation_actions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_conversation_actions.md)
- [with_navigation_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts.md)
- [with_navigation_shortcut_identities](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcut_identities.md)
- [with_named_property_mappings](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_named_property_mappings.md)
- [with_associated_configs](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [with_associated_config_identity_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_config_identity_ids.md)
- [with_delegate_freebusy_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages.md)
- [with_recoverable_items](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items.md)
- [with_reminders](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_reminders.md)
- [with_content_windows](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows.md)
- [with_calendar_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_calendar_attachments.md)
- [with_mailbox_content_commit_times](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_mailbox_content_commit_times.md)
- [with_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_identity_codec.md)

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)