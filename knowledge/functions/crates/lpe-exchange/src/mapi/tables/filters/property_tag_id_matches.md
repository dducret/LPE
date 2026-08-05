---
type: Rust Function
title: property_tag_id_matches
resource: crates/lpe-exchange/src/mapi/tables/filters.rs#L35-L37
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/filters/is_unrestricted_common_views_navigation_projection
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_property_by_id
---

# Signature

`pub(super) fn property_tag_id_matches(left: u32, right: u32) -> bool`

# Called by

- [is_unrestricted_common_views_navigation_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/is_unrestricted_common_views_navigation_projection.md)
- [navigation_shortcut_property_by_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_property_by_id.md)