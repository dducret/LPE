---
type: Rust Function
title: configuration_uses_xml_stream
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L978-L989
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_roaming_datatypes
---

# Signature

`fn configuration_uses_xml_stream(message_class: &str) -> bool`

# Calls

- [is_outlook_configuration_message_class_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name.md)

# Called by

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [configuration_roaming_datatypes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_roaming_datatypes.md)