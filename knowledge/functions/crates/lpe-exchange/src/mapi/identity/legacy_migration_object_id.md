---
type: Rust Function
title: legacy_migration_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L1049-L1055
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_matches
  - functions/crates/lpe-exchange/src/mapi_store/mapi_recoverable_item_id
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_reminders_table_projects_canonical_mixed_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/abort_submit_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_hard_delete_messages_reports_partial_when_retention_blocks_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_delete_persisted_search_folder_removes_definition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
---

# Signature

`pub(crate) fn legacy_migration_object_id(canonical_id: &Uuid) -> u64`

# Called by

- [object_id_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_matches.md)
- [mapi_recoverable_item_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_recoverable_item_id.md)
- [mapi_collaboration_folder_id_for_collection](../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection.md)
- [mapi_over_http_set_properties_updates_canonical_event_and_task_reminders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders.md)
- [mapi_over_http_reminders_table_projects_canonical_mixed_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_reminders_table_projects_canonical_mixed_rows.md)
- [abort_submit_response](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/abort_submit_response.md)
- [mapi_over_http_hard_delete_messages_reports_partial_when_retention_blocks_delete](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_hard_delete_messages_reports_partial_when_retention_blocks_delete.md)
- [mapi_over_http_contacts_sync_exports_associated_config_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes.md)
- [mapi_over_http_contact_content_sync_exports_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes.md)
- [mapi_over_http_delete_persisted_search_folder_removes_definition](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_delete_persisted_search_folder_removes_definition.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [fake_mapi_identity_lookup_for_object_id](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id.md)
- [fetch_or_allocate_mapi_identities](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)