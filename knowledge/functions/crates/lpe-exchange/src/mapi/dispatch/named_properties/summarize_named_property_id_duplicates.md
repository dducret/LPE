---
type: Rust Function
title: summarize_named_property_id_duplicates
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L673-L724
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/named_property_duplicate_summary_separates_repeats_from_collisions
---

# Signature

`pub(super) fn summarize_named_property_id_duplicates( properties: &[MapiNamedProperty], property_ids: &[u16], ) -> (usize, usize, usize, String)`

# Calls

- [format_debug_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [named_property_duplicate_summary_separates_repeats_from_collisions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/named_property_duplicate_summary_separates_repeats_from_collisions.md)