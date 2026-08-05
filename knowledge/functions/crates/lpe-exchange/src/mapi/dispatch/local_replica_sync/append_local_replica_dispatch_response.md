---
type: Rust Function
title: append_local_replica_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/local_replica_sync.rs#L147-L185
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_local_replica_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) -> bool where S: ExchangeStore,`

# Calls

- [append_set_local_replica_midset_deleted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response.md)
- [append_get_local_replica_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)