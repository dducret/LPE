---
type: Rust Function
title: nspi_filter_entries_for_request
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1207-L1217
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
---

# Signature

`pub(in crate::mapi) fn nspi_filter_entries_for_request( account_id: Uuid, entries: Vec<ExchangeAddressBookEntry>, request: &[u8], ) -> Vec<ExchangeAddressBookEntry>`

# Calls

- [resolve_names_requested_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
- [nspi_ranked_matching_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries.md)

# Called by

- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)