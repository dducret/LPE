---
type: Rust Function
title: hierarchy_semantic_validation
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L1068-L1260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/root_inclusive_idset
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_semantic_validation
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters
---

# Signature

`pub(crate) fn hierarchy_semantic_validation( sync_root_folder_id: u64, summary: &HierarchyTransferDebugSummary, ) -> HierarchySemanticValidation`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [format_debug_hex](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [counter_difference](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference.md)
- [root_inclusive_idset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/root_inclusive_idset.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [format_replguid_globset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)

# Called by

- [hierarchy_transfer_close_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary.md)
- [log_hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_semantic_validation.md)
- [hierarchy_transfer_debug_decoder_summarizes_serialized_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream.md)
- [hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters.md)