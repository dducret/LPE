---
type: Rust Function
title: append_set_local_replica_midset_deleted_response
resource: crates/lpe-exchange/src/mapi/dispatch/local_replica_sync.rs#L5-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_deleted_ranges
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_mapi_local_replica_deleted_ranges
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response
---

# Signature

`pub(super) async fn append_set_local_replica_midset_deleted_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [local_replica_deleted_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_deleted_ranges.md)
- [add_mapi_local_replica_deleted_ranges](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_mapi_local_replica_deleted_ranges.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)

# Called by

- [append_local_replica_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response.md)