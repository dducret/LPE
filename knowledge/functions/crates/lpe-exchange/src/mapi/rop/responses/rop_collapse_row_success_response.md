---
type: Rust Function
title: rop_collapse_row_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L347-L358
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response
---

# Signature

`pub(in crate::mapi) fn rop_collapse_row_success_response( request: &RopRequest, collapsed_row_count: usize, ) -> Vec<u8>`

# Called by

- [rop_collapse_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response.md)