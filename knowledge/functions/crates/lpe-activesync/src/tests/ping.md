---
type: Rust Function
title: ping
resource: crates/lpe-activesync/src/tests.rs#L2113-L2124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/active_sync_query
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  called_by:
  - functions/crates/lpe-activesync/src/tests/ping_recoverable_errors_return_documented_statuses
  - functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state
  - functions/crates/lpe-activesync/src/tests/ping_empty_request_without_cached_parameters_returns_missing_parameters
  - functions/crates/lpe-activesync/src/tests/ping_invalid_folder_id_requires_folder_sync
  - functions/crates/lpe-activesync/src/tests/ping_invalid_folder_class_requires_folder_sync
  - functions/crates/lpe-activesync/src/tests/ping_no_changes_returns_no_change_status
  - functions/crates/lpe-activesync/src/tests/ping_reports_changed_folder_ids_as_folder_values
  - functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections
  - functions/crates/lpe-activesync/src/tests/ping_heartbeat_outside_supported_range_returns_limit
  - functions/crates/lpe-activesync/src/tests/ping_too_many_monitored_folders_returns_max_folders
  - functions/crates/lpe-activesync/src/tests/ping_surfaces_hierarchy_change_as_folder_sync_required
---

# Signature

`async fn ping(service: &ActiveSyncService<FakeStore>, device_id: &str, body: &[u8]) -> WbxmlNode`

# Calls

- [active_sync_query](../../../../../functions/crates/lpe-activesync/src/tests/active_sync_query.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)

# Called by

- [ping_recoverable_errors_return_documented_statuses](../../../../../functions/crates/lpe-activesync/src/tests/ping_recoverable_errors_return_documented_statuses.md)
- [ping_reconnects_after_service_restart_using_persisted_sync_state](../../../../../functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state.md)
- [ping_empty_request_without_cached_parameters_returns_missing_parameters](../../../../../functions/crates/lpe-activesync/src/tests/ping_empty_request_without_cached_parameters_returns_missing_parameters.md)
- [ping_invalid_folder_id_requires_folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_id_requires_folder_sync.md)
- [ping_invalid_folder_class_requires_folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_class_requires_folder_sync.md)
- [ping_no_changes_returns_no_change_status](../../../../../functions/crates/lpe-activesync/src/tests/ping_no_changes_returns_no_change_status.md)
- [ping_reports_changed_folder_ids_as_folder_values](../../../../../functions/crates/lpe-activesync/src/tests/ping_reports_changed_folder_ids_as_folder_values.md)
- [ping_detects_changes_across_multiple_monitored_collections](../../../../../functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections.md)
- [ping_heartbeat_outside_supported_range_returns_limit](../../../../../functions/crates/lpe-activesync/src/tests/ping_heartbeat_outside_supported_range_returns_limit.md)
- [ping_too_many_monitored_folders_returns_max_folders](../../../../../functions/crates/lpe-activesync/src/tests/ping_too_many_monitored_folders_returns_max_folders.md)
- [ping_surfaces_hierarchy_change_as_folder_sync_required](../../../../../functions/crates/lpe-activesync/src/tests/ping_surfaces_hierarchy_change_as_folder_sync_required.md)