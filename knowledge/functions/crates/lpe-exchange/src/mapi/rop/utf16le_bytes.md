---
type: Rust Function
title: utf16le_bytes
resource: crates/lpe-exchange/src/mapi/rop.rs#L846-L852
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/view_descriptor_value_shape_for_debug
---

# Signature

`fn utf16le_bytes(value: &str) -> Vec<u8>`

# Called by

- [log_common_view_descriptor_getprops_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [view_descriptor_value_shape_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/view_descriptor_value_shape_for_debug.md)