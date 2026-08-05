---
type: Rust Function
title: active_sync_query
resource: crates/lpe-activesync/src/tests.rs#L1880-L1887
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds
  - functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably
  - functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version
  - functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
  - functions/crates/lpe-activesync/src/tests/permissive_mode_preserves_current_unprovisioned_behavior
  - functions/crates/lpe-activesync/src/tests/folder_command
  - functions/crates/lpe-activesync/src/tests/sync_collection
  - functions/crates/lpe-activesync/src/tests/ping
  - functions/crates/lpe-activesync/src/tests/move_items_invalid_source_and_destination_return_item_statuses
  - functions/crates/lpe-activesync/src/tests/item_operations_missing_and_unknown_file_reference_return_attachment_status
  - functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2
---

# Signature

`fn active_sync_query(cmd: &str, device_id: &str) -> ActiveSyncQuery`

# Called by

- [post_with_supported_protocol_version_succeeds](../../../../../functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds.md)
- [post_with_unsupported_protocol_version_is_rejected_predictably](../../../../../functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably.md)
- [unsupported_protocol_version_response_does_not_echo_request_version](../../../../../functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)
- [permissive_mode_preserves_current_unprovisioned_behavior](../../../../../functions/crates/lpe-activesync/src/tests/permissive_mode_preserves_current_unprovisioned_behavior.md)
- [folder_command](../../../../../functions/crates/lpe-activesync/src/tests/folder_command.md)
- [sync_collection](../../../../../functions/crates/lpe-activesync/src/tests/sync_collection.md)
- [ping](../../../../../functions/crates/lpe-activesync/src/tests/ping.md)
- [move_items_invalid_source_and_destination_return_item_statuses](../../../../../functions/crates/lpe-activesync/src/tests/move_items_invalid_source_and_destination_return_item_statuses.md)
- [item_operations_missing_and_unknown_file_reference_return_attachment_status](../../../../../functions/crates/lpe-activesync/src/tests/item_operations_missing_and_unknown_file_reference_return_attachment_status.md)
- [search_malformed_range_returns_store_status_2](../../../../../functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2.md)