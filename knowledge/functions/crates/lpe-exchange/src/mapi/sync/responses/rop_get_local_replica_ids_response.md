---
type: Rust Function
title: rop_get_local_replica_ids_response
resource: crates/lpe-exchange/src/mapi/sync/responses.rs#L96-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_release_rops_without_responses
---

# Signature

`pub(in crate::mapi) fn rop_get_local_replica_ids_response( request: &RopRequest, first_global_counter: u64, ) -> Vec<u8>`

# Calls

- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)

# Called by

- [append_get_local_replica_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response.md)
- [execute_rop_debug_summary_skips_release_rops_without_responses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_release_rops_without_responses.md)