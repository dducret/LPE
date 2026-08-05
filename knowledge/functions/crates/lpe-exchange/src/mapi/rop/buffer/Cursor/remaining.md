---
type: Rust Method
title: remaining
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L78-L80
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncread/poll_read
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining_is_zero_padding
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`pub(in crate::mapi) fn remaining(&self) -> usize`

# Called by

- [poll_read](../../../../../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncread/poll_read.md)
- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [additional_ren_entry_ids_from_profile_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes.md)
- [summarize_request_rop_raw_frames](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames.md)
- [summarize_request_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [rop_buffer_is_store_independent_release_only](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [fast_transfer_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values.md)
- [parse_dn_to_mid_names](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [parse_nspi_get_props_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request.md)
- [read_ascii_z](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [remaining_is_zero_padding](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining_is_zero_padding.md)
- [import_move](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
- [import_read_state_changes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [parse_mapi_restriction](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction.md)
- [plan_mapi_store_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)