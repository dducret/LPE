---
type: Rust Function
title: append_logon_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L8-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_logon_dispatch_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, typed_request: &TypedRopRequest, principal: &AccountPrincipal, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) -> bool`

# Calls

- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)
- [append_address_types_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)