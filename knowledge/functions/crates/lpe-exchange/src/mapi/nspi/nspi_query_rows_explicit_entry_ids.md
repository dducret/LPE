---
type: Rust Function
title: nspi_query_rows_explicit_entry_ids
resource: crates/lpe-exchange/src/mapi/nspi.rs#L800-L815
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
---

# Signature

`pub(in crate::mapi) fn nspi_query_rows_explicit_entry_ids( request_type: &str, request: &[u8], ) -> Vec<u32>`

# Calls

- [nspi_query_rows_count_details](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [nspi_word_looks_like_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)

# Called by

- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [log_nspi_rowset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)