---
type: Rust Function
title: remember_mapi_identity_with_source_key
resource: crates/lpe-exchange/src/mapi/identity.rs#L672-L691
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/remember
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
---

# Signature

`pub(crate) fn remember_mapi_identity_with_source_key( canonical_id: Uuid, object_id: u64, source_key: Option<Vec<u8>>, )`

# Calls

- [current_mapi_request_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities.md)
- [remember](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/remember.md)

# Called by

- [persist_associated_config_message](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [calendar_move_to_deleted_items_partial_completion](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [append_get_per_user_long_term_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response.md)
- [mapi_object_ids_for_deleted_changes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)
- [remember_created_mapi_identity_record](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)
- [append_synchronization_import_message_move_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [saved_message_handle_getprops_keeps_batch_email_and_durable_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity.md)
- [load_mapi_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)
- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [contacts_project_exactly_the_persisted_contact_link_fai](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)