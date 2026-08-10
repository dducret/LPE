---
type: Rust Function
title: replguid_idset_from_object_ids
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1423-L1430
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/durable_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
---

# Signature

`fn replguid_idset_from_object_ids(ids: &[u64]) -> Vec<u8>`

# Calls

- [durable_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/durable_object_id.md)
- [replguid_idset_from_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)

# Called by

- [final_sync_state_stream_with_cnsets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)