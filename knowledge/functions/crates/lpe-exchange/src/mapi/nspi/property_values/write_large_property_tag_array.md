---
type: Rust Function
title: write_large_property_tag_array
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L728-L733
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table
---

# Signature

`pub(in crate::mapi) fn write_large_property_tag_array(body: &mut Vec<u8>, tags: &[u32])`

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_property_tags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response.md)
- [write_embedded_address_book_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table.md)