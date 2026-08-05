---
type: Rust Function
title: configuration_roaming_datatypes
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L953-L976
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
---

# Signature

`fn configuration_roaming_datatypes( message_class: &str, properties: &HashMap<u32, MapiValue>, ) -> u32`

# Calls

- [configuration_uses_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream.md)

# Called by

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)