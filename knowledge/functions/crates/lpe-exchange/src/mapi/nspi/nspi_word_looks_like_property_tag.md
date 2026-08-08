---
type: Rust Function
title: nspi_word_looks_like_property_tag
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1402-L1420
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id
---

# Signature

`fn nspi_word_looks_like_property_tag(value: u32) -> bool`

# Calls

- [nspi_property_tag_is_supported](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported.md)

# Called by

- [nspi_word_looks_like_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)