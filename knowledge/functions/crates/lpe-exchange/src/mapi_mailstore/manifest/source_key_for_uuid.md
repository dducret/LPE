---
type: Rust Function
title: source_key_for_uuid
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L269-L276
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contents_find_row_matches_message_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/source_and_change_keys_are_stable_replica_scoped_values
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/unchanged_object_keeps_source_key_and_changed_object_advances_change_number
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys
---

# Signature

`pub(crate) fn source_key_for_uuid(id: &Uuid) -> Vec<u8>`

# Calls

- [mapped_mapi_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [contact_property_value_with_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [imported_hierarchy_existing_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox.md)
- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [note_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value.md)
- [journal_entry_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [task_property_value_with_reminder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [normal_message_sync_facts_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [contents_find_row_matches_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contents_find_row_matches_message_search_key.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [source_and_change_keys_are_stable_replica_scoped_values](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/source_and_change_keys_are_stable_replica_scoped_values.md)
- [unchanged_object_keeps_source_key_and_changed_object_advances_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/unchanged_object_keeps_source_key_and_changed_object_advances_change_number.md)
- [mapi_over_http_cached_mode_properties_include_canonical_change_keys](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys.md)