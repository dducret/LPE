---
type: Rust Function
title: nspi_entry_property_value_list
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L367-L383
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/address_book_tagged_values_use_mapi_http_layout
---

# Signature

`pub(in crate::mapi) fn nspi_entry_property_value_list( account_id: Uuid, entry: &ExchangeAddressBookEntry, tags: &[u32], directory_entries: &[ExchangeAddressBookEntry], ) -> Vec<u8>`

# Calls

- [write_address_book_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value.md)
- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)

# Called by

- [nspi_template_info_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
- [address_book_tagged_values_use_mapi_http_layout](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/address_book_tagged_values_use_mapi_http_layout.md)