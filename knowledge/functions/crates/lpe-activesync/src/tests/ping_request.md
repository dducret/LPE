---
type: Rust Function
title: ping_request
resource: crates/lpe-activesync/src/tests.rs#L2094-L2112
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/ping_recoverable_errors_return_documented_statuses
  - functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state
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

`fn ping_request(heartbeat: Option<&str>, folders: &[(&str, &str)]) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [ping_recoverable_errors_return_documented_statuses](../../../../../functions/crates/lpe-activesync/src/tests/ping_recoverable_errors_return_documented_statuses.md)
- [ping_reconnects_after_service_restart_using_persisted_sync_state](../../../../../functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state.md)
- [ping_invalid_folder_id_requires_folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_id_requires_folder_sync.md)
- [ping_invalid_folder_class_requires_folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_class_requires_folder_sync.md)
- [ping_no_changes_returns_no_change_status](../../../../../functions/crates/lpe-activesync/src/tests/ping_no_changes_returns_no_change_status.md)
- [ping_reports_changed_folder_ids_as_folder_values](../../../../../functions/crates/lpe-activesync/src/tests/ping_reports_changed_folder_ids_as_folder_values.md)
- [ping_detects_changes_across_multiple_monitored_collections](../../../../../functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections.md)
- [ping_heartbeat_outside_supported_range_returns_limit](../../../../../functions/crates/lpe-activesync/src/tests/ping_heartbeat_outside_supported_range_returns_limit.md)
- [ping_too_many_monitored_folders_returns_max_folders](../../../../../functions/crates/lpe-activesync/src/tests/ping_too_many_monitored_folders_returns_max_folders.md)
- [ping_surfaces_hierarchy_change_as_folder_sync_required](../../../../../functions/crates/lpe-activesync/src/tests/ping_surfaces_hierarchy_change_as_folder_sync_required.md)