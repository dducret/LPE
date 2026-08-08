---
type: Rust Function
title: nspi_body_looks_like_query_rows
resource: crates/lpe-exchange/src/mapi/nspi.rs#L840-L842
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details
---

# Signature

`fn nspi_body_looks_like_query_rows(request: &[u8]) -> bool`

# Calls

- [nspi_query_rows_layout_from_body](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body.md)

# Called by

- [nspi_query_rows_count_details](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)