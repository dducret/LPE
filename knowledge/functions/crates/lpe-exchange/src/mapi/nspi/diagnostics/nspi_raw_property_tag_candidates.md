---
type: Rust Function
title: nspi_raw_property_tag_candidates
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L107-L123
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/nspi_word_looks_like_requested_property_tag
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(super) fn nspi_raw_property_tag_candidates(request: &[u8]) -> Vec<u32>`

# Calls

- [nspi_word_looks_like_requested_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/nspi_word_looks_like_requested_property_tag.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)