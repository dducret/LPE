---
type: Rust Function
title: nspi_filter_explicit_table_entries
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1222-L1236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/query_rows_explicit_table_filters_rows_by_requested_mid
---

# Signature

`fn nspi_filter_explicit_table_entries( account_id: Uuid, entries: Vec<ExchangeAddressBookEntry>, requested_entry_ids: &[u32], ) -> Vec<ExchangeAddressBookEntry>`

# Calls

- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)

# Called by

- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [query_rows_explicit_table_filters_rows_by_requested_mid](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/query_rows_explicit_table_filters_rows_by_requested_mid.md)