---
type: Rust Function
title: outlook_view_descriptor_clsid
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L140-L150
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
---

# Signature

`pub(in crate::mapi) fn outlook_view_descriptor_clsid(folder_id: u64) -> [u8; 16]`

# Called by

- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)