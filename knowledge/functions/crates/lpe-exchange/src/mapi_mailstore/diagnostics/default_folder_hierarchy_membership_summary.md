---
type: Rust Function
title: default_folder_hierarchy_membership_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L655-L696
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_specs
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders
---

# Signature

`pub(crate) fn default_folder_hierarchy_membership_summary( sync_type: u8, sync_root_folder_id: u64, transfer_buffer: &[u8], ) -> String`

# Calls

- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [default_folder_hierarchy_membership_specs](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_specs.md)
- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders.md)