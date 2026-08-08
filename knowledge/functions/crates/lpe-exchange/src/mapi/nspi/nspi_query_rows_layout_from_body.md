---
type: Rust Function
title: nspi_query_rows_layout_from_body
resource: crates/lpe-exchange/src/mapi/nspi.rs#L844-L853
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_at_offset
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_body_looks_like_query_rows
---

# Signature

`fn nspi_query_rows_layout_from_body(request: &[u8]) -> Option<NspiQueryRowsCountDetails>`

# Calls

- [nspi_query_rows_layout_at_offset](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_at_offset.md)

# Called by

- [nspi_query_rows_count_details](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)
- [nspi_body_looks_like_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_body_looks_like_query_rows.md)