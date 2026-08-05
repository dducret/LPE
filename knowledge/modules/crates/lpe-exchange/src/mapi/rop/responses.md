---
type: Rust Module
title: responses
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L1-L754
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-default-folder-property-tags-with-identity-message-for-id-pending-text-property-search-folder-message-for-id
  - external/super-rop-error-response-write-object-id-write-typed-string-write-typed-string-reduced-unicode-when-lossless-write-u16-write-u32-write-u64-roprequest
  - external/crate-mapi-identity-outbox-folder-id
  - external/crate-mapi-identity-inbox-folder-id-root-folder-id
  - external/crate-mapi-properties
  - external/crate-mapi-session-mapiobject
  - external/crate-mapi-tables-default-attachment-columns-default-contact-property-tags-default-conversation-action-property-tags-default-event-property-tags-default-folder-property-tags-default-journal-entry-property-tags-default-message-property-tags-default-note-property-tags-default-store-property-tags-default-task-property-tags-message-recipients-serialize-recipient-row-write-standard-property-row
  - external/crate-mapi-wire-ropid
  - external/crate-mapi-store-mapimailstoresnapshot
  - external/lpe-storage-jmapemail-jmapmailbox
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_open_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_folder_response.md)
- [rop_open_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response.md)
- [rop_open_message_response_with_named_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties.md)
- [rop_open_message_response_with_recipients](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients.md)
- [rop_open_embedded_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_embedded_message_response.md)
- [rop_message_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_message_status_response.md)
- [rop_create_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_create_folder_response.md)
- [rop_get_hierarchy_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_hierarchy_table_response.md)
- [rop_get_contents_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_contents_table_response.md)
- [rop_get_attachment_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_attachment_table_response.md)
- [rop_open_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_attachment_response.md)
- [rop_create_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_create_attachment_response.md)
- [rop_open_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_stream_response.md)
- [rop_read_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response.md)
- [rop_seek_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response.md)
- [rop_write_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_stream_response.md)
- [rop_copy_to_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_copy_to_stream_response.md)
- [rop_get_stream_size_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_stream_size_response.md)
- [rop_get_address_types_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_address_types_response.md)
- [rop_transport_send_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_transport_send_success_response.md)
- [rop_options_data_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_options_data_response.md)
- [rop_partial_completion_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)
- [rop_set_columns_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_columns_response.md)
- [rop_sort_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_sort_table_response.md)
- [rop_expand_row_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response.md)
- [rop_collapse_row_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_collapse_row_success_response.md)
- [rop_get_collapse_state_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_collapse_state_success_response.md)
- [rop_set_collapse_state_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_collapse_state_success_response.md)
- [rop_restrict_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_restrict_response.md)
- [rop_create_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_create_message_response.md)
- [rop_set_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_response.md)
- [rop_set_properties_problem_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_problem_response.md)
- [rop_delete_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_delete_properties_response.md)
- [rop_simple_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [rop_get_search_criteria_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_search_criteria_response.md)
- [rop_upload_state_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_upload_state_success_response.md)
- [rop_fast_transfer_put_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_fast_transfer_put_buffer_response.md)
- [rop_save_changes_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_save_changes_message_response.md)
- [rop_set_read_flags_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_read_flags_response.md)
- [rop_get_per_user_long_term_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_long_term_ids_response.md)
- [rop_get_per_user_guid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_guid_response.md)
- [rop_read_per_user_information_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response.md)
- [rop_write_per_user_information_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_per_user_information_response.md)
- [rop_get_transport_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_transport_folder_response.md)
- [rop_get_store_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_store_state_response.md)
- [rop_get_owning_servers_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_owning_servers_response.md)
- [rop_public_folder_is_ghosted_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_public_folder_is_ghosted_response.md)
- [rop_reset_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reset_table_response.md)
- [rop_reload_cached_information_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)
- [rop_get_properties_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response.md)

# Imports

- `super::{
    default_folder_property_tags_with_identity, message_for_id, pending_text_property,
    search_folder_message_for_id,
}`
- `super::{
    rop_error_response, write_object_id, write_typed_string,
    write_typed_string_reduced_unicode_when_lossless, write_u16, write_u32, write_u64, RopRequest,
}`
- `crate::mapi::identity::OUTBOX_FOLDER_ID`
- `crate::mapi::identity::{INBOX_FOLDER_ID, ROOT_FOLDER_ID}`
- `crate::mapi::properties::*`
- `crate::mapi::session::MapiObject`
- `crate::mapi::tables::{
    default_attachment_columns, default_contact_property_tags,
    default_conversation_action_property_tags, default_event_property_tags,
    default_folder_property_tags, default_journal_entry_property_tags,
    default_message_property_tags, default_note_property_tags, default_store_property_tags,
    default_task_property_tags, message_recipients, serialize_recipient_row,
    write_standard_property_row,
}`
- `crate::mapi::wire::RopId`
- `crate::mapi_store::MapiMailStoreSnapshot`
- `lpe_storage::{JmapEmail, JmapMailbox}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)