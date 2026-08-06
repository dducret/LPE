---
type: Rust Function
title: is_outlook_logon_bootstrap_getprops
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L560-L602
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
---

# Signature

`pub(in crate::mapi) fn is_outlook_logon_bootstrap_getprops( object: Option<&MapiObject>, columns: &[u32], ) -> bool`

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)