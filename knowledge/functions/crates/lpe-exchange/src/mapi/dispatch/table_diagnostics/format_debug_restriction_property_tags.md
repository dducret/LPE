---
type: Rust Function
title: format_debug_restriction_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L328-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/collect_restriction_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
---

# Signature

`pub(super) fn format_debug_restriction_property_tags( restriction: Option<&MapiRestriction>, ) -> String`

# Calls

- [collect_restriction_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/collect_restriction_property_tags.md)
- [format_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)

# Called by

- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)