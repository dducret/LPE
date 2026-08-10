---
type: Rust Function
title: final_sync_state_stream_with_cnsets
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L650-L678
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_content_sync_state_stream
---

# Signature

`fn final_sync_state_stream_with_cnsets( sync_type: u8, object_ids: &[u64], normal_change_numbers: &[u64], fai_change_numbers: &[u64], read_change_numbers: &[u64], ) -> Vec<u8>`

# Calls

- [replguid_idset_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids.md)
- [replguid_idset_from_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [write_binary_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [final_sync_state_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [final_content_sync_state_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_content_sync_state_stream.md)