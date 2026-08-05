---
type: Rust Function
title: rop_get_owning_servers_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L571-L585
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response
---

# Signature

`pub(in crate::mapi) fn rop_get_owning_servers_response( request: &RopRequest, servers: &[String], ) -> Vec<u8>`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_get_owning_servers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response.md)