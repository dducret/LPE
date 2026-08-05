---
type: Rust Function
title: rop_not_content_restriction
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L910-L922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause
---

# Signature

`fn rop_not_content_restriction(property_tag: u32, fuzzy_level_low: u16, value: &str) -> Vec<u8>`

# Calls

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [rop_restriction_from_json_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)