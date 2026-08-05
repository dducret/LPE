---
type: Rust Function
title: nspi_get_props_property_value_list
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L385-L415
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(super) fn nspi_get_props_property_value_list( account_id: Uuid, entry: &ExchangeAddressBookEntry, tags: &[u32], directory_entries: &[ExchangeAddressBookEntry], ) -> (Vec<u8>, bool)`

# Calls

- [nspi_property_tag_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported.md)
- [write_address_book_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value.md)
- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)

# Called by

- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)