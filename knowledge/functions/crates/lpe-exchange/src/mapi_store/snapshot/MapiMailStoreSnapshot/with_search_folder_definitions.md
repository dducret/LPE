---
type: Rust Method
title: with_search_folder_definitions
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L126-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_is_projectable
  - functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_definition_to_folder
  - functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_projection_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_persisted_search_folder_contract
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_prefers_saved_search_definition
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_returns_search_type_for_saved_search_definition
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_projects_saved_search_definition_metadata
  - functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_search_getprops_content_count_matches_projected_results
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_folder_message_count_matches_projected_results
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosrch_common_views_projects_search_folder_definition_messages
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_skips_search_folder_definition_without_protocol_blob
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_search_folder_definition_with_protocol_blob
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_outlook_contact_books_into_fixed_mapi_folders
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_carries_persisted_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_resolves_tracked_mail_processing_by_advertised_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_user_saved_search_folder_as_mapi_folder
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_deduplicates_user_saved_search_folder_projection_by_name
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_ignores_blank_mapi_bounded_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_tasks_into_todo_search_results
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_followup_mail_into_todo_search_results
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_reminders_as_underlying_calendar_and_task_links
---

# Signature

`pub(crate) fn with_search_folder_definitions( mut self, search_folder_definitions: Vec<SearchFolderDefinition>, ) -> Self`

# Calls

- [user_saved_search_folder_is_projectable](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_is_projectable.md)
- [mapi_search_folder_definition_to_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_definition_to_folder.md)
- [user_saved_search_folder_projection_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_projection_key.md)

# Called by

- [folder_properties_for_open_projects_persisted_search_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_persisted_search_folder_contract.md)
- [folder_type_getprops_contract_prefers_saved_search_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_type_getprops_contract_prefers_saved_search_definition.md)
- [folder_getprops_returns_search_type_for_saved_search_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_returns_search_type_for_saved_search_definition.md)
- [folder_getprops_projects_saved_search_definition_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_projects_saved_search_definition_metadata.md)
- [contacts_search_getprops_content_count_matches_projected_results](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_search_getprops_content_count_matches_projected_results.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [contacts_search_folder_message_count_matches_projected_results](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_folder_message_count_matches_projected_results.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [microsoft_oxosrch_common_views_projects_search_folder_definition_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosrch_common_views_projects_search_folder_definition_messages.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [common_views_skips_search_folder_definition_without_protocol_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_skips_search_folder_definition_without_protocol_blob.md)
- [common_views_projects_search_folder_definition_with_protocol_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_search_folder_definition_with_protocol_blob.md)
- [snapshot_projects_outlook_contact_books_into_fixed_mapi_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_outlook_contact_books_into_fixed_mapi_folders.md)
- [snapshot_carries_persisted_search_folder_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_carries_persisted_search_folder_definitions.md)
- [snapshot_resolves_tracked_mail_processing_by_advertised_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_resolves_tracked_mail_processing_by_advertised_folder_id.md)
- [snapshot_projects_user_saved_search_folder_as_mapi_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_user_saved_search_folder_as_mapi_folder.md)
- [snapshot_deduplicates_user_saved_search_folder_projection_by_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_deduplicates_user_saved_search_folder_projection_by_name.md)
- [snapshot_ignores_blank_mapi_bounded_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_ignores_blank_mapi_bounded_user_saved_search_folder.md)
- [snapshot_projects_canonical_tasks_into_todo_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_tasks_into_todo_search_results.md)
- [snapshot_projects_followup_mail_into_todo_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_followup_mail_into_todo_search_results.md)
- [snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results.md)
- [snapshot_projects_reminders_as_underlying_calendar_and_task_links](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_reminders_as_underlying_calendar_and_task_links.md)