---
type: Rust Function
title: rop_get_per_user_guid_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L517-L525
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response
---

# Signature

`pub(in crate::mapi) fn rop_get_per_user_guid_response( request: &RopRequest, database_guid: &[u8; 16], ) -> Vec<u8>`

# Called by

- [append_get_per_user_guid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response.md)