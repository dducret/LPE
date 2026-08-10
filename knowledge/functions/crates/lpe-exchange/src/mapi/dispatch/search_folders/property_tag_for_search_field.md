---
type: Rust Function
title: property_tag_for_search_field
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L961-L984
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/string_search_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/multiple_string_search_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction
---

# Signature

`fn property_tag_for_search_field(field: &str, use_unicode: bool) -> Result<u32, u32>`

# Calls

- [string_search_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/string_search_property_tag.md)
- [multiple_string_search_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/multiple_string_search_property_tag.md)

# Called by

- [rop_restriction_from_json_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)
- [rop_property_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction.md)