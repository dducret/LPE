---
type: Rust Function
title: nspi_word_looks_like_entry_id
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1397-L1400
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_at_offset
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id
---

# Signature

`fn nspi_word_looks_like_entry_id(value: u32) -> bool`

# Calls

- [nspi_word_looks_like_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_property_tag.md)

# Called by

- [nspi_query_rows_explicit_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids.md)
- [nspi_query_rows_layout_at_offset](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_at_offset.md)
- [nspi_stat_current_rec](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [nspi_direct_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id.md)