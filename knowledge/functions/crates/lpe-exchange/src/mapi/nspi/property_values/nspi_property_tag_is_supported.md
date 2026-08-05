---
type: Rust Function
title: nspi_property_tag_is_supported
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L251-L254
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_property_tag
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list
---

# Signature

`pub(in crate::mapi) fn nspi_property_tag_is_supported(tag: u32) -> bool`

# Called by

- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_direct_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id.md)
- [nspi_word_looks_like_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_property_tag.md)
- [nspi_requested_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags.md)
- [nspi_get_props_property_value_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list.md)