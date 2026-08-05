---
type: Rust Function
title: common_view_descriptor_property_requested
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L899-L913
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values
---

# Signature

`pub(in crate::mapi) fn common_view_descriptor_property_requested(columns: &[u32]) -> bool`

# Called by

- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_response_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values.md)