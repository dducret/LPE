---
type: Rust Function
title: conversation_action_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L1179-L1254
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_string_array
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys
  - functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn conversation_action_property_value( message: &MapiConversationActionMessage, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [conversation_action_subject](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject.md)
- [conversation_action_size](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_size.md)
- [mapi_message_size_extended_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [conversation_index_for_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [json_string_array](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_string_array.md)

# Called by

- [associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys.md)
- [conversation_action_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)
- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [serialize_conversation_action_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row.md)
- [restricted_associated_folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)