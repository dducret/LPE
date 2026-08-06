---
type: Rust Function
title: collect_restriction_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L419-L453
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags
---

# Signature

`fn collect_restriction_property_tags(restriction: &MapiRestriction, tags: &mut Vec<u32>)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [restriction_property_tags_from_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request.md)
- [format_debug_restriction_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags.md)