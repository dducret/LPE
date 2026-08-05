---
type: Rust Function
title: log_mapi_session_disconnect
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L718-L1810
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
  - functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  - functions/crates/lpe-exchange/src/mapi/session/types/logon_identity_matches_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/recent_execute_debug_summaries
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/special_folder_contract_summary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/required_default_folder_disconnect_coverage_summary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/partial_scope_checkpoint_not_stored_count
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_fai_inbox_probe_loop_terminal_summary
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/abandoned_after_inbox_fai_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
---

# Signature

`pub(in crate::mapi) fn log_mapi_session_disconnect( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, session_id: &str, session: &MapiSession, request_id: &str, request_type: &str, )`

# Calls

- [post_hierarchy_action_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)
- [guid_counter_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)
- [client_flow_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)
- [cookie_value_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)
- [hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)
- [logon_identity_matches_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/types/logon_identity_matches_store_replica_guid.md)
- [recent_execute_debug_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/recent_execute_debug_summaries.md)
- [special_folder_contract_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/special_folder_contract_summary.md)
- [required_default_folder_disconnect_coverage_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/required_default_folder_disconnect_coverage_summary.md)
- [partial_scope_checkpoint_not_stored_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/partial_scope_checkpoint_not_stored_count.md)
- [post_fai_inbox_probe_loop_terminal_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_fai_inbox_probe_loop_terminal_summary.md)
- [abandoned_after_inbox_fai_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/abandoned_after_inbox_fai_query_rows.md)
- [outlook_startup_gate_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)
- [normal_inbox_visible_row_missing_reason](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason.md)
- [normal_inbox_visible_row_release_request_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape.md)
- [advertised_default_view_pending_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open.md)
- [advertised_default_view_pending_open_is_primary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary.md)
- [default_view_advertisement_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state.md)
- [default_view_advertisement_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary.md)
- [visible_inbox_release_without_query_rows_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed.md)

# Called by

- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)