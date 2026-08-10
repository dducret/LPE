---
type: Rust Function
title: write_globset_ranges
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1479-L1486
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets
---

# Signature

`fn write_globset_ranges(buffer: &mut Vec<u8>, ranges: &[(u64, u64)])`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [replguid_idset_from_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [replid_idset_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)
- [write_replid_idset_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property.md)
- [encode_replguid_sets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets.md)