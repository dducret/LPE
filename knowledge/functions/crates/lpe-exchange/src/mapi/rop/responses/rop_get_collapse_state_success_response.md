---
type: Rust Function
title: rop_get_collapse_state_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L360-L372
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response
---

# Signature

`pub(in crate::mapi) fn rop_get_collapse_state_success_response( request: &RopRequest, collapse_state: &[u8], ) -> Vec<u8>`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [rop_get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response.md)