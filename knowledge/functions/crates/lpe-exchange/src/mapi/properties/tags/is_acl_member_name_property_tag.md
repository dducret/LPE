---
type: Rust Function
title: is_acl_member_name_property_tag
resource: crates/lpe-exchange/src/mapi/properties/tags.rs#L117-L119
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name
  - functions/crates/lpe-exchange/src/mapi/rop/debug/property_tag_debug_name
---

# Signature

`pub(in crate::mapi) fn is_acl_member_name_property_tag(property_tag: u32) -> bool`

# Called by

- [set_property_debug_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name.md)
- [property_tag_debug_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/property_tag_debug_name.md)