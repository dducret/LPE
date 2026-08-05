---
type: Rust Function
title: append_stream_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/stream_dispatch.rs#L25-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_stream_dispatch_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)
- [append_copy_to_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)
- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [append_commit_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)