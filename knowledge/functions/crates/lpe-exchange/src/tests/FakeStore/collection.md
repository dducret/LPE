---
type: Rust Method
title: collection
resource: crates/lpe-exchange/src/tests/mod.rs#L4287-L4299
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/rights
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/create_update_task_round_trips_through_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/get_sharing_folder_returns_accessible_same_tenant_calendar_grant
  - functions/crates/lpe-exchange/src/tests/ews/refresh_sharing_folder_verifies_accessible_shared_contacts_folder
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contacts_from_canonical_store
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contact_update_for_legacy_id_only_sync_state
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_no_contact_change_for_current_keyed_sync_state
  - functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync
  - functions/crates/lpe-exchange/src/tests/ews/update_contact_round_trips_through_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/update_contact_unmapped_field_still_advances_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/find_item_returns_calendar_items_from_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_reminders_table_projects_canonical_mixed_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_associated_contents_columns_include_configuration_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions
---

# Signature

`fn collection(id: &str, kind: &str, display_name: &str) -> CollaborationCollection`

# Calls

- [rights](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/rights.md)

# Called by

- [create_update_task_round_trips_through_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_update_task_round_trips_through_sync_folder_items.md)
- [get_sharing_folder_returns_accessible_same_tenant_calendar_grant](../../../../../../functions/crates/lpe-exchange/src/tests/ews/get_sharing_folder_returns_accessible_same_tenant_calendar_grant.md)
- [refresh_sharing_folder_verifies_accessible_shared_contacts_folder](../../../../../../functions/crates/lpe-exchange/src/tests/ews/refresh_sharing_folder_verifies_accessible_shared_contacts_folder.md)
- [sync_folder_items_returns_contacts_from_canonical_store](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contacts_from_canonical_store.md)
- [sync_folder_items_returns_contact_update_for_legacy_id_only_sync_state](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contact_update_for_legacy_id_only_sync_state.md)
- [sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state.md)
- [sync_folder_items_returns_no_contact_change_for_current_keyed_sync_state](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_returns_no_contact_change_for_current_keyed_sync_state.md)
- [ews_contact_change_key_is_stable_across_get_find_and_sync](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync.md)
- [update_contact_round_trips_through_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/update_contact_round_trips_through_sync_folder_items.md)
- [update_contact_unmapped_field_still_advances_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/update_contact_unmapped_field_still_advances_sync_folder_items.md)
- [find_item_returns_calendar_items_from_canonical_store](../../../../../../functions/crates/lpe-exchange/src/tests/ews/find_item_returns_calendar_items_from_canonical_store.md)
- [mapi_over_http_set_properties_updates_canonical_event_and_task_reminders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders.md)
- [mapi_over_http_reminders_table_projects_canonical_mixed_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_reminders_table_projects_canonical_mixed_rows.md)
- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint.md)
- [mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration.md)
- [mapi_over_http_calendar_associated_contents_columns_include_configuration_properties](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_associated_contents_columns_include_configuration_properties.md)
- [mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_shared_calendar_read_only_rights_reject_mutations](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations.md)
- [mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_contacts_search_content_sync_uses_search_folder_parent](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent.md)
- [mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts.md)
- [mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions.md)