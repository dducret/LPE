---
type: Rust Function
title: bounded_search_not_clause
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L621-L641
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses
---

# Signature

`fn bounded_search_not_clause(restriction: &MapiRestriction) -> Result<Value, u32>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [bounded_search_restriction_clauses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses.md)