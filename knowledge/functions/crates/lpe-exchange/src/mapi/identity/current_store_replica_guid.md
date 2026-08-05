---
type: Rust Function
title: current_store_replica_guid
resource: crates/lpe-exchange/src/mapi/identity.rs#L53-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid
---

# Signature

`pub(crate) fn current_store_replica_guid() -> [u8; 16]`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)

# Called by

- [log_execute_rop_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [append_get_per_user_guid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response.md)
- [imported_message_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [source_key_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)
- [rop_logon_response_body](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)
- [rop_public_folder_logon_response_body](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body.md)
- [rop_get_local_replica_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response.md)
- [serialized_replid_guid_map](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [local](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local.md)
- [local_mut](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut.md)
- [replguid_idset_from_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [replguid_globset_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [counter_from_xid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid.md)