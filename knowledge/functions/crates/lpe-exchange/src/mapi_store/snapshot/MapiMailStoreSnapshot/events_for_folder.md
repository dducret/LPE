---
type: Rust Method
title: events_for_folder
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L972-L977
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids
  - functions/crates/lpe-exchange/src/tests/mapi_calendar_event_identity_survives_restart_style_store_reload
---

# Signature

`pub(crate) fn events_for_folder(&self, folder_id: u64) -> Vec<&MapiEvent>`

# Called by

- [log_calendar_identity_chain](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [calendar_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event.md)
- [mapi_over_http_shared_calendar_read_only_rights_reject_mutations](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations.md)
- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
- [calendar_notification_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids.md)
- [mapi_calendar_event_identity_survives_restart_style_store_reload](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_calendar_event_identity_survives_restart_style_store_reload.md)