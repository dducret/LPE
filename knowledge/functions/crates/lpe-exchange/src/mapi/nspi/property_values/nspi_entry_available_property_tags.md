---
type: Rust Function
title: nspi_entry_available_property_tags
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L258-L347
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_available_properties_omit_embedded_tables_for_skip_objects
---

# Signature

`pub(in crate::mapi) fn nspi_entry_available_property_tags( entry: &ExchangeAddressBookEntry, flags: u32, ) -> Vec<u32>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_get_props_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags.md)
- [nspi_available_properties_omit_embedded_tables_for_skip_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_available_properties_omit_embedded_tables_for_skip_objects.md)