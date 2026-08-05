---
type: Rust Function
title: append_table_open_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_open.rs#L11-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_table_open_dispatch_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, logon_id: u8, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [append_receive_folder_table_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)