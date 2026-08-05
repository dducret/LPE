---
type: Rust Function
title: append_address_types_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L216-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_dispatch_response
---

# Signature

`pub(super) fn append_address_types_dispatch_response( principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) -> bool`

# Calls

- [append_address_types_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)

# Called by

- [append_logon_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_dispatch_response.md)