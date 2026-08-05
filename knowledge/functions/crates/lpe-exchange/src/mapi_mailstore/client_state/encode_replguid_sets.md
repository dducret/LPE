---
type: Rust Function
title: encode_replguid_sets
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1148-L1158
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state
---

# Signature

`fn encode_replguid_sets(sets: &ReplicaCounterSets) -> Vec<u8>`

# Calls

- [write_globset_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges.md)

# Called by

- [replguid_idset_from_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [write_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state.md)