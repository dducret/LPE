---
type: Rust Function
title: rop_get_per_user_long_term_ids_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L504-L515
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response
---

# Signature

`pub(in crate::mapi) fn rop_get_per_user_long_term_ids_response( request: &RopRequest, long_term_ids: &[[u8; 24]], ) -> Vec<u8>`

# Called by

- [append_get_per_user_long_term_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response.md)