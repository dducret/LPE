---
type: Rust Function
title: rop_get_hierarchy_table_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L138-L146
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_hierarchy_table_response
---

# Signature

`pub(in crate::mapi) fn rop_get_hierarchy_table_response( request: &RopRequest, row_count: u32, ) -> Vec<u8>`

# Called by

- [get_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_hierarchy_table_response.md)