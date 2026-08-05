---
type: Rust Function
title: property_position
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L752-L758
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_identity_properties_before_display_name
---

# Signature

`pub(super) fn property_position(property_tags: &[u32], property_tag: u32) -> usize`

# Calls

- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)
- [hierarchy_identity_properties_before_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_identity_properties_before_display_name.md)