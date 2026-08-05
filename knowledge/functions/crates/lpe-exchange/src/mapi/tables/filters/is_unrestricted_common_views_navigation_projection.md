---
type: Rust Function
title: is_unrestricted_common_views_navigation_projection
resource: crates/lpe-exchange/src/mapi/tables/filters.rs#L39-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/filters/property_tag_id_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn is_unrestricted_common_views_navigation_projection( columns: &[u32], restriction: &Option<MapiRestriction>, ) -> bool`

# Calls

- [property_tag_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/property_tag_id_matches.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)