---
type: Rust Function
title: log_nspi_response_contract
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L225-L264
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response
---

# Signature

`pub(super) fn log_nspi_response_contract( principal: &AccountPrincipal, request_type: &str, request_id: &str, method_return_value: u32, body: &[u8], rowset_present: bool, returned_row_count: usize, property_tags: &[u32], context: &str, )`

# Called by

- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response.md)