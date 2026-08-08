---
type: Rust Function
title: nspi_match_dn_to_mid_entry
resource: crates/lpe-exchange/src/mapi/nspi.rs#L535-L545
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match
---

# Signature

`fn nspi_match_dn_to_mid_entry<'a>( entries: &'a [ExchangeAddressBookEntry], value: &str, ) -> Option<&'a ExchangeAddressBookEntry>`

# Calls

- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [nspi_entry_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)

# Called by

- [nspi_dn_to_mid_match](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)