---
type: Rust Function
title: nspi_entry_match_rank
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1307-L1343
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries
---

# Signature

`pub(in crate::mapi) fn nspi_entry_match_rank( entry: &ExchangeAddressBookEntry, value: &str, ) -> Option<u8>`

# Calls

- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [nspi_entry_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)

# Called by

- [nspi_match_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)
- [nspi_ranked_matching_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries.md)