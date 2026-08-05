---
type: Rust Function
title: append_public_folder_replica_probe_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L606-L628
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_metadata_dispatch_response
---

# Signature

`pub(super) fn append_public_folder_replica_probe_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [append_get_owning_servers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response.md)
- [append_public_folder_is_ghosted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response.md)

# Called by

- [append_public_folder_metadata_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_metadata_dispatch_response.md)