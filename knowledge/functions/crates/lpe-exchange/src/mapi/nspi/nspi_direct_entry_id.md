---
type: Rust Function
title: nspi_direct_entry_id
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1380-L1392
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
---

# Signature

`fn nspi_direct_entry_id(request: &[u8]) -> Option<u32>`

# Calls

- [nspi_word_looks_like_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)
- [nspi_property_tag_is_supported](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported.md)

# Called by

- [nspi_request_has_entry_selector](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector.md)
- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)