---
type: Rust Function
title: mapi_object_debug_fields
resource: crates/lpe-exchange/src/mapi/rop/debug/shapes.rs#L74-L319
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug
---

# Signature

`pub(in crate::mapi) fn mapi_object_debug_fields( object: Option<&MapiObject>, ) -> (&'static str, String, String)`

# Called by

- [log_get_properties_specific_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [log_calendar_default_folder_lookup_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug.md)