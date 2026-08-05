---
type: Rust Function
title: search_folder_message_for_id
resource: crates/lpe-exchange/src/mapi/properties.rs#L165-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_message_body_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response
---

# Signature

`pub(in crate::mapi) fn search_folder_message_for_id( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<&MapiMessage>`

# Calls

- [todo_search_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_message_for_id.md)
- [tracked_mail_processing_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_message_for_id.md)
- [reminder_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_message_for_id.md)

# Called by

- [log_message_getprops_response_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug.md)
- [append_open_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [rop_read_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [format_message_body_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_message_body_getprops_contract.md)
- [rop_reload_cached_information_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)