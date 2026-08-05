---
type: Rust Function
title: property_value_by_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L47-L54
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/common_views_saved_shortcut_summary
---

# Signature

`fn property_value_by_id( properties: &HashMap<u32, MapiValue>, property_tag: u32, ) -> Option<&MapiValue>`

# Calls

- [property_ids_match](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match.md)

# Called by

- [common_views_saved_shortcut_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/common_views_saved_shortcut_summary.md)