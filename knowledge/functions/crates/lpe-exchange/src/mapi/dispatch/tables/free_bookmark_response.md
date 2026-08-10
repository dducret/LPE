---
type: Rust Function
title: free_bookmark_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1357-L1362
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response
---

# Signature

`pub(super) fn free_bookmark_response( request: &RopRequest, object: Option<&mut MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response.md)

# Called by

- [append_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response.md)