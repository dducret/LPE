---
type: Rust Function
title: nspi_entry_record_key
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1049-L1051
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
---

# Signature

`fn nspi_entry_record_key(entry: &ExchangeAddressBookEntry) -> Vec<u8>`

# Calls

- [nspi_entry_permanent_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)

# Called by

- [nspi_entry_value_with_directory](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)