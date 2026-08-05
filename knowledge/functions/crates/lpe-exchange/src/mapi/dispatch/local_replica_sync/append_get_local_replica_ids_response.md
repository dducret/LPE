---
type: Rust Function
title: append_get_local_replica_ids_response
resource: crates/lpe-exchange/src/mapi/dispatch/local_replica_sync.rs#L73-L145
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_id_count
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response
---

# Signature

`pub(super) async fn append_get_local_replica_ids_response<S>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [local_replica_id_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_id_count.md)
- [reserve_mapi_local_replica_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [rop_get_local_replica_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response.md)

# Called by

- [append_local_replica_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response.md)