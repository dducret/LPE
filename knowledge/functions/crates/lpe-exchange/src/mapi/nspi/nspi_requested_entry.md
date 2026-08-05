---
type: Rust Function
title: nspi_requested_entry
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1182-L1199
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(in crate::mapi) fn nspi_requested_entry<'a>( account_id: Uuid, request: &[u8], entries: &'a [ExchangeAddressBookEntry], ) -> Option<&'a ExchangeAddressBookEntry>`

# Calls

- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)
- [nspi_match_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)

# Called by

- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)