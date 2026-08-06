---
type: Rust Function
title: format_requested_view_descriptor_contract
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L922-L957
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
---

# Signature

`pub(in crate::mapi) fn format_requested_view_descriptor_contract(columns: &[u32]) -> String`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)