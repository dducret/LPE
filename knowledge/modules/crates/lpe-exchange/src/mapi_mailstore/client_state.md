---
type: Rust Module
title: client_state
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1-L1293
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-btreemap
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [CounterSet](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet.md)
- [ReplicaCounterSets](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets.md)
- [SyncStateSets](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/SyncStateSets.md)
- [DownloadChangeFact](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/DownloadChangeFact.md)
- [download_change_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [ParsedProperty](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/ParsedProperty.md)
- [ProgressPerMessage](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/ProgressPerMessage.md)
- [ManifestChange](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/ManifestChange.md)
- [ParsedManifest](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/client_state/ParsedManifest.md)
- [from_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges.md)
- [contains](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/contains.md)
- [insert](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/insert.md)
- [union_with](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with.md)
- [difference](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/difference.md)
- [intersection](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection.md)
- [is_empty](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/is_empty.md)
- [local](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local.md)
- [local_mut](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut.md)
- [insert](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/insert.md)
- [source_key_replica_counter](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter.md)
- [replguid_idset_from_source_keys](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [validate_download_state_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/validate_download_state_property.md)
- [select_download_manifest_for_client_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [parse_manifest](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
- [parse_change](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change.md)
- [parse_standalone_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_standalone_state.md)
- [parse_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)
- [required_state_value](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/required_state_value.md)
- [decode_replguid_set](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set.md)
- [decode_replid_set](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set.md)
- [decode_globset_range_prefix](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix.md)
- [globcnt_suffix_range](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/globcnt_suffix_range.md)
- [parse_progress_mode](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_mode.md)
- [parse_progress_per_message](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_per_message.md)
- [write_selected_progress_mode](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_selected_progress_mode.md)
- [parse_read_state_section](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section.md)
- [parse_deletion_section](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section.md)
- [write_deletion_section](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_deletion_section.md)
- [write_replid_idset_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property.md)
- [write_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state.md)
- [encode_replguid_sets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets.md)
- [parse_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)
- [fixed_property_range](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/fixed_property_range.md)
- [variable_property_range](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/variable_property_range.md)
- [multi_string_property_range](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/multi_string_property_range.md)
- [parse_bool](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_bool.md)
- [read_u32](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/read_u32.md)
- [is_change_boundary](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_change_boundary.md)
- [is_fast_transfer_marker](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_fast_transfer_marker.md)

# Imports

- `std::collections::BTreeMap`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)