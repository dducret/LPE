---
type: Rust Method
title: as_i64
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L467-L488
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/values/json_i64_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object
---

# Signature

`pub(in crate::mapi) fn as_i64(&self) -> Option<i64>`

# Calls

- [try_from](../../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [persist_associated_config_message](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [bounded_search_property_clause](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause.md)
- [compare_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [reject_unsupported_mapi_event_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties.md)
- [message_followup_update_from_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [split_reminder_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [mapi_value_from_json](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json.md)
- [json_i64_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/json_i64_values.md)
- [write_mapi_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [search_folder_definition_sync_object](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)