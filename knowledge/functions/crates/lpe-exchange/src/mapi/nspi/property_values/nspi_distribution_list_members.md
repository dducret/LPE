---
type: Rust Function
title: nspi_distribution_list_members
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L526-L547
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
---

# Signature

`fn nspi_distribution_list_members( entry: &ExchangeAddressBookEntry, directory_entries: &[ExchangeAddressBookEntry], ) -> Vec<ExchangeAddressBookEntry>`

# Called by

- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)