---
type: Rust Function
title: replguid_idset_from_counters
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1429-L1439
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges
  - functions/crates/lpe-exchange/src/mapi_mailstore/coalesced_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/root_inclusive_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers
---

# Signature

`fn replguid_idset_from_counters(counters: &[u64]) -> Vec<u8>`

# Calls

- [current_store_replica_guid](../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [write_globset_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges.md)
- [coalesced_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/coalesced_ranges.md)

# Called by

- [upload_sync_state_stream_from_sets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)
- [final_sync_state_stream_with_cnsets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)
- [replguid_idset_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids.md)
- [root_inclusive_idset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/root_inclusive_idset.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes.md)
- [content_sync_state_keeps_normal_and_fai_cnsets_separate](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)