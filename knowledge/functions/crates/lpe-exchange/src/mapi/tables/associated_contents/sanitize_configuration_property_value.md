---
type: Rust Function
title: sanitize_configuration_property_value
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L422-L438
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
---

# Signature

`fn sanitize_configuration_property_value( message_class: &str, property_tag: u32, value: MapiValue, ) -> MapiValue`

# Calls

- [is_outlook_configuration_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [minimal_roaming_dictionary_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream.md)

# Called by

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)