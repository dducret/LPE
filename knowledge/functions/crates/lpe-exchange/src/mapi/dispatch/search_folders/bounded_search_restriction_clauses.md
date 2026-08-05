---
type: Rust Function
title: bounded_search_restriction_clauses
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L428-L471
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_content_clause
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_not_clause
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
---

# Signature

`fn bounded_search_restriction_clauses(restriction: &MapiRestriction) -> Result<Vec<Value>, u32>`

# Calls

- [bounded_search_content_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_content_clause.md)
- [bounded_search_not_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_not_clause.md)
- [bounded_search_property_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)