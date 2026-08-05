---
type: Rust Function
title: sync_state_stream_from_raw_properties
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L609-L628
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
---

# Signature

`fn sync_state_stream_from_raw_properties( sync_type: u8, idset_given: &[u8], cnset_seen: &[u8], cnset_seen_fai: &[u8], cnset_read: &[u8], ) -> Vec<u8>`

# Calls

- [write_binary_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [sync_state_stream_with_uploaded_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)