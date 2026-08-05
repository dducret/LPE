---
type: Rust Function
title: legacy_dn_recipient_address
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L285-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_matches_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
---

# Signature

`fn legacy_dn_recipient_address( legacy_dn: &str, principal: &AccountPrincipal, address_book_entries: &[ExchangeAddressBookEntry], ) -> Option<String>`

# Calls

- [normalize_nspi_lookup_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [legacy_dn_matches_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_matches_entry.md)
- [nspi_entry_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)

# Called by

- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)