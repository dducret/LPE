---
type: Rust Function
title: nspi_match_entry
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1235-L1261
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_kind_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry
---

# Signature

`pub(in crate::mapi) fn nspi_match_entry<'a>( account_id: Uuid, entries: &'a [ExchangeAddressBookEntry], value: &str, ) -> Option<&'a ExchangeAddressBookEntry>`

# Calls

- [nspi_entry_match_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank.md)
- [nspi_entry_kind_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_kind_rank.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)

# Called by

- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_requested_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)