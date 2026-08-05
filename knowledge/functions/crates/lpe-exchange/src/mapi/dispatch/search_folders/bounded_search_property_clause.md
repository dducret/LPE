---
type: Rust Function
title: bounded_search_property_clause
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L643-L709
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/filetime_to_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses
---

# Signature

`fn bounded_search_property_clause( relop: u8, property_tag: u32, value: &MapiValue, ) -> Result<Value, u32>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [filetime_to_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/filetime_to_rfc3339_utc.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)

# Called by

- [bounded_search_restriction_clauses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses.md)