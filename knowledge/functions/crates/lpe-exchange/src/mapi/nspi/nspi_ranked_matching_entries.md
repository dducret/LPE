---
type: Rust Function
title: nspi_ranked_matching_entries
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1266-L1300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_kind_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request
---

# Signature

`fn nspi_ranked_matching_entries( account_id: Uuid, entries: Vec<ExchangeAddressBookEntry>, values: &[String], ) -> Vec<ExchangeAddressBookEntry>`

# Calls

- [nspi_entry_match_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank.md)
- [nspi_entry_kind_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_kind_rank.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)

# Called by

- [nspi_filter_entries_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request.md)