---
type: Rust Function
title: logon_request_handle
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L161-L171
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response
---

# Signature

`pub(super) fn logon_request_handle( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, ) -> bool`

# Calls

- [private_logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)

# Called by

- [append_get_owning_servers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response.md)
- [append_public_folder_is_ghosted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response.md)