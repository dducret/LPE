---
type: Rust Function
title: log_calendar_special_sync_objects
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L74-L312
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_calendar_configuration_object
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/special_binary_property_len
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/calendar_sync_object_start_end_order_ok
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_calendar_special_sync_objects( principal: &AccountPrincipal, folder_id: u64, sync_type: u8, objects: &[mapi_mailstore::SpecialMessageSyncFact], )`

# Calls

- [special_message_source_key](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key.md)
- [is_calendar_configuration_object](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_calendar_configuration_object.md)
- [is_outlook_configuration_message_class_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name.md)
- [special_binary_property_len](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/special_binary_property_len.md)
- [calendar_sync_object_start_end_order_ok](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/calendar_sync_object_start_end_order_ok.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)