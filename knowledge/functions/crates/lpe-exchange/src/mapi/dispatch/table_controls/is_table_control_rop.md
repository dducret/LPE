---
type: Rust Function
title: is_table_control_rop
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L50-L78
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn is_table_control_rop( rop_id: RopId, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, ) -> bool`

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)