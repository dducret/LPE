---
type: Rust Function
title: nspi_query_rows_count
resource: crates/lpe-exchange/src/mapi/nspi.rs#L796-L798
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
---

# Signature

`pub(in crate::mapi) fn nspi_query_rows_count(request_type: &str, request: &[u8]) -> Option<usize>`

# Calls

- [nspi_query_rows_count_details](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)

# Called by

- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)