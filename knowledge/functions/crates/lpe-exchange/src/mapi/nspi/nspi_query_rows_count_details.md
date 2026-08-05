---
type: Rust Function
title: nspi_query_rows_count_details
resource: crates/lpe-exchange/src/mapi/nspi.rs#L821-L829
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_type_is_query_rows
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_body_looks_like_query_rows
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
---

# Signature

`fn nspi_query_rows_count_details( request_type: &str, request: &[u8], ) -> Option<NspiQueryRowsCountDetails>`

# Calls

- [nspi_request_type_is_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_type_is_query_rows.md)
- [nspi_body_looks_like_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_body_looks_like_query_rows.md)
- [nspi_query_rows_layout_from_body](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body.md)

# Called by

- [nspi_query_rows_count](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count.md)
- [nspi_query_rows_explicit_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids.md)
- [log_nspi_rowset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)