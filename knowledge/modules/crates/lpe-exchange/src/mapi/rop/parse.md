---
type: Rust Module
title: parse
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1-L1491
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-parse-mapi-restriction-read-u16-prefixed-string-rop-id-is-reserved-typed-requests-cursor
  - external/crate-mapi-properties-canonical-property-storage-tag-parse-mapi-property-value-mapinamedproperty-mapinamedpropertykind-mapirestriction-mapisortorder-mapivalue-pid-tag-source-key
  - external/crate-mapi-wire-ropid
  - external/crate-store-mapilocalreplicadeletedrange
  - external/anyhow-anyhow-result
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [RopRequest](../../../../../../classes/crates/lpe-exchange/src/mapi/rop/parse/RopRequest.md)
- [ImportMessageMove](../../../../../../classes/crates/lpe-exchange/src/mapi/rop/parse/ImportMessageMove.md)
- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [output_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/output_handle_index.md)
- [response_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_id.md)
- [create_message_associated](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_message_associated.md)
- [abort_submit_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_folder_id.md)
- [abort_submit_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_message_id.md)
- [public_folder_probe_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/public_folder_probe_object_id.md)
- [notification_types](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types.md)
- [notification_want_whole_store](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store.md)
- [notification_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id.md)
- [message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_id.md)
- [row_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/row_id.md)
- [read_recipients_reserved](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_recipients_reserved.md)
- [attach_num](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/attach_num.md)
- [stream_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_property_tag.md)
- [stream_open_mode](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_open_mode.md)
- [read_byte_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_byte_count.md)
- [stream_write_data](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_write_data.md)
- [stream_seek_origin](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_origin.md)
- [stream_seek_offset](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_offset.md)
- [stream_size](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_size.md)
- [read_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_flags.md)
- [want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_asynchronous.md)
- [sync_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_flags.md)
- [sync_extra_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_extra_flags.md)
- [sync_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sync_property_tags.md)
- [fast_transfer_buffer_size](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_buffer_size.md)
- [fast_transfer_uses_server_determined_buffer_size](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_uses_server_determined_buffer_size.md)
- [stream_data](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_data.md)
- [fast_transfer_upload_data](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_upload_data.md)
- [upload_state_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_property_tag.md)
- [upload_state_transfer_size](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_transfer_size.md)
- [import_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id.md)
- [import_flag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_flag.md)
- [import_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values.md)
- [import_hierarchy_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values.md)
- [import_delete_message_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids.md)
- [import_delete_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)
- [import_delete_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_flags.md)
- [import_delete_hard_delete](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_hard_delete.md)
- [fast_transfer_message_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_message_ids.md)
- [import_move](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
- [import_read_state_changes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [local_replica_deleted_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_deleted_ranges.md)
- [search_criteria_restriction_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_restriction_bytes.md)
- [search_criteria_folder_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_folder_ids.md)
- [search_criteria_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_flags.md)
- [get_search_criteria_include_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_restriction.md)
- [get_search_criteria_use_unicode](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_use_unicode.md)
- [get_search_criteria_include_folders](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_folders.md)
- [receive_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/receive_folder_message_class.md)
- [set_receive_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_id.md)
- [set_receive_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_message_class.md)
- [local_replica_id_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_id_count.md)
- [long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id.md)
- [per_user_folder_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_folder_object_id.md)
- [per_user_data_offset](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_data_offset.md)
- [per_user_max_data_size](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_max_data_size.md)
- [per_user_has_finished](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_has_finished.md)
- [per_user_write_data](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_write_data.md)
- [message_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids.md)
- [delete_messages_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_want_asynchronous.md)
- [delete_messages_notify_non_read](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_notify_non_read.md)
- [status_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/status_message_id.md)
- [message_status_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_flags.md)
- [message_status_mask](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_mask.md)
- [reload_cached_information_reserved](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/reload_cached_information_reserved.md)
- [create_folder_type](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_type.md)
- [create_folder_open_existing](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_open_existing.md)
- [create_folder_reserved](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_reserved.md)
- [create_folder_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_display_name.md)
- [delete_folder_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_flags.md)
- [delete_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_id.md)
- [move_copy_message_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids.md)
- [move_copy_want_copy](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy.md)
- [move_copy_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_asynchronous.md)
- [move_copy_want_copy_raw](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy_raw.md)
- [folder_move_copy_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_folder_id.md)
- [folder_move_copy_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_asynchronous.md)
- [folder_move_copy_want_recursive](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_recursive.md)
- [folder_move_copy_use_unicode](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_use_unicode.md)
- [folder_move_copy_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_display_name.md)
- [empty_folder_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_asynchronous.md)
- [empty_folder_want_delete_associated](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_delete_associated.md)
- [move_copy_target_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [copy_to_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_to_want_asynchronous.md)
- [copy_to_want_subobjects](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_to_want_subobjects.md)
- [copy_to_excluded_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_to_excluded_property_tags.md)
- [copy_properties_want_asynchronous](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_properties_want_asynchronous.md)
- [copy_properties_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_properties_property_tags.md)
- [query_row_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [query_no_advance](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_no_advance.md)
- [query_forward_read](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)
- [find_origin](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_origin.md)
- [find_backward](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward.md)
- [bookmark](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark.md)
- [bookmark_row_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_row_count.md)
- [bookmark_want_row_moved_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_want_row_moved_count.md)
- [seek_origin](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_origin.md)
- [seek_row_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_row_count.md)
- [want_row_moved_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_row_moved_count.md)
- [fractional_position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fractional_position.md)
- [sort_orders](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders.md)
- [sort_category_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_category_count.md)
- [sort_expanded_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_expanded_count.md)
- [category_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/category_id.md)
- [expand_max_row_count](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/expand_max_row_count.md)
- [collapse_state](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state.md)
- [collapse_state_row_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_id.md)
- [collapse_state_row_instance_number](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_instance_number.md)
- [property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [property_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_ids.md)
- [named_property_create](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_create.md)
- [named_property_names](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_names.md)
- [named_property_query_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_query_guid.md)
- [property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_values.md)
- [read_nonempty_u32_prefixed_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/read_nonempty_u32_prefixed_bytes.md)
- [rop_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/rop_id.md)
- [unsupported_is_terminal](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/unsupported_is_terminal.md)
- [typed](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [parse_tagged_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property_value.md)
- [parse_tagged_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)
- [parse_named_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property.md)
- [decode_utf16z_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/decode_utf16z_bytes.md)
- [parse_property_value_for_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)

# Imports

- `super::{
    parse_mapi_restriction, read_u16_prefixed_string, rop_id_is_reserved, typed_requests::*, Cursor,
}`
- `crate::mapi::properties::{
    canonical_property_storage_tag, parse_mapi_property_value, MapiNamedProperty,
    MapiNamedPropertyKind, MapiRestriction, MapiSortOrder, MapiValue, PID_TAG_SOURCE_KEY,
}`
- `crate::mapi::wire::RopId`
- `crate::store::MapiLocalReplicaDeletedRange`
- `anyhow::{anyhow, Result}`
- `uuid::Uuid`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)