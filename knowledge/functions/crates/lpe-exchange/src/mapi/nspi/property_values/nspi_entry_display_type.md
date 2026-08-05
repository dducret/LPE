---
type: Rust Function
title: nspi_entry_display_type
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L709-L717
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type_ex
  - functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id
---

# Signature

`pub(in crate::mapi) fn nspi_entry_display_type(entry: &ExchangeAddressBookEntry) -> u32`

# Called by

- [nspi_entry_permanent_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)
- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)
- [nspi_entry_display_type_ex](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type_ex.md)
- [mailbox_owner_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id.md)