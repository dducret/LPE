---
type: Rust Function
title: nspi_query_rows_layout_at_offset
resource: crates/lpe-exchange/src/mapi/nspi.rs#L852-L888
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body
---

# Signature

`fn nspi_query_rows_layout_at_offset( request: &[u8], etable_count_offset: usize, ) -> Option<NspiQueryRowsCountDetails>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [nspi_word_looks_like_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)

# Called by

- [nspi_query_rows_layout_from_body](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body.md)