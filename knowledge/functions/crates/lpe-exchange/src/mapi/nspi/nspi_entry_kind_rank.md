---
type: Rust Function
title: nspi_entry_kind_rank
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1299-L1305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries
---

# Signature

`fn nspi_entry_kind_rank(entry_kind: ExchangeAddressBookEntryKind) -> u8`

# Called by

- [nspi_match_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)
- [nspi_ranked_matching_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries.md)