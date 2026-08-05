---
type: Rust Function
title: append_public_folder_metadata_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L642-L680
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_replica_probe_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_public_folder_metadata_dispatch_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [append_public_folder_per_user_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response.md)
- [append_public_folder_replica_probe_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_replica_probe_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)