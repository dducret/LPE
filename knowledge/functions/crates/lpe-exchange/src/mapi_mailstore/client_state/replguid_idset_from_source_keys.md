---
type: Rust Function
title: replguid_idset_from_source_keys
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L316-L330
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
---

# Signature

`pub(super) fn replguid_idset_from_source_keys<'a>( source_keys: impl IntoIterator<Item = (&'a [u8], u64)>, ) -> Vec<u8>`

# Calls

- [source_key_replica_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter.md)
- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [current_store_replica_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [encode_replguid_sets](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets.md)

# Called by

- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)