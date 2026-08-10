---
type: Rust Function
title: coalesced_ranges
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1459-L1477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids
---

# Signature

`fn coalesced_ranges(counters: &[u64]) -> Vec<(u64, u64)>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [replguid_idset_from_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [replid_idset_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)