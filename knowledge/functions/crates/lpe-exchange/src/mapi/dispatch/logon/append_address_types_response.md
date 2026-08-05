---
type: Rust Function
title: append_address_types_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L181-L214
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/address_types_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response
---

# Signature

`pub(super) fn append_address_types_response( principal: &AccountPrincipal, session: &MapiSession, object: Option<&MapiObject>, request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [address_types_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/address_types_response.md)

# Called by

- [append_address_types_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response.md)