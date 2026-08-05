---
type: Rust Function
title: is_calendar_configuration_object
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L465-L479
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
---

# Signature

`pub(in crate::mapi::dispatch) fn is_calendar_configuration_object( object: &mapi_mailstore::SpecialMessageSyncFact, ) -> bool`

# Calls

- [is_outlook_configuration_message_class_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name.md)

# Called by

- [log_calendar_special_sync_objects](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)