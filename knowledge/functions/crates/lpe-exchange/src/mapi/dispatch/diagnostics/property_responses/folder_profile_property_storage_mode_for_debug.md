---
type: Rust Function
title: folder_profile_property_storage_mode_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L117-L137
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug
---

# Signature

`fn folder_profile_property_storage_mode_for_debug( object: Option<&MapiObject>, property_tags: &[u32], property_value_shapes: &str, ) -> String`

# Called by

- [log_set_properties_specific_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug.md)