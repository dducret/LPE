---
type: Rust Function
title: is_store_independent_special_folder
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L142-L153
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe
---

# Signature

`fn is_store_independent_special_folder(folder_id: u64) -> bool`

# Called by

- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [is_store_independent_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe.md)