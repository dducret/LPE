---
type: Rust Function
title: is_outlook_configuration_message_class_name
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L522-L527
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_calendar_configuration_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status
---

# Signature

`pub(crate) fn is_outlook_configuration_message_class_name( message_class: &str, expected: &str, ) -> bool`

# Called by

- [log_calendar_special_sync_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)
- [is_calendar_configuration_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_calendar_configuration_object.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [configuration_uses_xml_stream](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream.md)
- [special_message_status](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status.md)