---
type: Rust Function
title: restriction_property_tags_from_request
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L239-L250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/collect_restriction_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
---

# Signature

`pub(super) fn restriction_property_tags_from_request(request: &RopRequest) -> Vec<u32>`

# Calls

- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)
- [collect_restriction_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/collect_restriction_property_tags.md)

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)