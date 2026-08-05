---
type: Rust Function
title: nspi_get_props_property_tags
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L237-L249
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_available_property_tags
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(super) fn nspi_get_props_property_tags( request: &[u8], entry: Option<&ExchangeAddressBookEntry>, ) -> Vec<u32>`

# Calls

- [parse_nspi_get_props_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request.md)
- [nspi_entry_available_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_available_property_tags.md)
- [nspi_requested_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags.md)

# Called by

- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)