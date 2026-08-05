---
type: Rust Function
title: hierarchy_identity_properties_before_display_name
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L760-L777
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_position
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
---

# Signature

`pub(crate) fn hierarchy_identity_properties_before_display_name(property_tags: &[u32]) -> bool`

# Calls

- [property_position](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_position.md)

# Called by

- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)