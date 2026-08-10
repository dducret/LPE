---
type: Rust Function
title: set_collapse_state_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1950-L1955
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(super) fn set_collapse_state_response( request: &RopRequest, object: Option<&mut MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)