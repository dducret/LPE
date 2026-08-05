---
type: Rust Function
title: string_search_property_tag
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L987-L993
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field
---

# Signature

`fn string_search_property_tag(property_tag: u32, use_unicode: bool) -> u32`

# Called by

- [rop_restriction_from_json_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)
- [property_tag_for_search_field](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field.md)