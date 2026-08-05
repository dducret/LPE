---
type: Rust Function
title: append_folder_open_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/folder_open.rs#L8-L36
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_folder_open_dispatch_response( principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, same_execute_released_handles: &HashSet<u32>, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)