---
type: Rust Method
title: collaboration_folders
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L890-L892
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_remove_deletes_calendar_grant
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions
---

# Signature

`pub(crate) fn collaboration_folders(&self) -> &[MapiCollaborationFolder]`

# Called by

- [log_calendar_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [hierarchy_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders.md)
- [mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event.md)
- [mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity.md)
- [mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_shared_calendar_read_only_rights_reject_mutations](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations.md)
- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
- [calendar_notification_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids.md)
- [mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_custom_calendar_modify_permissions_remove_deletes_calendar_grant](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_remove_deletes_calendar_grant.md)
- [mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions.md)