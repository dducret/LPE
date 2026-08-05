---
type: Rust Function
title: decode_response_body
resource: crates/lpe-activesync/src/tests.rs#L1859-L1862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/decode_wbxml
  called_by:
  - functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document
  - functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
  - functions/crates/lpe-activesync/src/tests/folder_command
  - functions/crates/lpe-activesync/src/tests/sync_collection
  - functions/crates/lpe-activesync/src/tests/folder_sync
  - functions/crates/lpe-activesync/src/tests/ping
  - functions/crates/lpe-activesync/src/tests/handle_sync_node
  - functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches
  - functions/crates/lpe-activesync/src/tests/move_items_invalid_source_and_destination_return_item_statuses
  - functions/crates/lpe-activesync/src/tests/item_operations_missing_and_unknown_file_reference_return_attachment_status
  - functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2
  - functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders
  - functions/crates/lpe-activesync/src/tests/sync_key_zero_primes_then_returns_paged_more_available_changes
  - functions/crates/lpe-activesync/src/tests/get_item_estimate_returns_pending_sync_count
  - functions/crates/lpe-activesync/src/tests/stable_sync_does_not_reload_full_email_payloads_without_changes
  - functions/crates/lpe-activesync/src/tests/sync_key_stays_usable_for_new_changes_after_a_stable_round
  - functions/crates/lpe-activesync/src/tests/stale_sync_key_is_rejected_after_a_completed_round
  - functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/send_mail_rejects_inaccessible_shared_mailbox_address
  - functions/crates/lpe-activesync/src/tests/item_operations_fetch_returns_attachment_bytes
  - functions/crates/lpe-activesync/src/tests/search_queries_canonical_mail_projection
  - functions/crates/lpe-activesync/src/tests/ping_reports_changed_collections_after_sync_state_exists
  - functions/crates/lpe-activesync/src/tests/ping_rejects_unsynchronized_folders
  - functions/crates/lpe-activesync/src/tests/smart_reply_uses_source_recipients_and_canonical_submission
---

# Signature

`async fn decode_response_body(response: axum::response::Response) -> WbxmlNode`

# Calls

- [decode_wbxml](../../../../../functions/crates/lpe-activesync/src/wbxml/decode_wbxml.md)

# Called by

- [provision_returns_policy_key_and_lightweight_policy_document](../../../../../functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)
- [folder_command](../../../../../functions/crates/lpe-activesync/src/tests/folder_command.md)
- [sync_collection](../../../../../functions/crates/lpe-activesync/src/tests/sync_collection.md)
- [folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync.md)
- [ping](../../../../../functions/crates/lpe-activesync/src/tests/ping.md)
- [handle_sync_node](../../../../../functions/crates/lpe-activesync/src/tests/handle_sync_node.md)
- [base64_sync_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches.md)
- [base64_ping_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches.md)
- [base64_smart_reply_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches.md)
- [base64_smart_forward_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches.md)
- [base64_move_items_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches.md)
- [move_items_invalid_source_and_destination_return_item_statuses](../../../../../functions/crates/lpe-activesync/src/tests/move_items_invalid_source_and_destination_return_item_statuses.md)
- [item_operations_missing_and_unknown_file_reference_return_attachment_status](../../../../../functions/crates/lpe-activesync/src/tests/item_operations_missing_and_unknown_file_reference_return_attachment_status.md)
- [search_malformed_range_returns_store_status_2](../../../../../functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2.md)
- [move_items_moves_message_between_canonical_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders.md)
- [sync_key_zero_primes_then_returns_paged_more_available_changes](../../../../../functions/crates/lpe-activesync/src/tests/sync_key_zero_primes_then_returns_paged_more_available_changes.md)
- [get_item_estimate_returns_pending_sync_count](../../../../../functions/crates/lpe-activesync/src/tests/get_item_estimate_returns_pending_sync_count.md)
- [stable_sync_does_not_reload_full_email_payloads_without_changes](../../../../../functions/crates/lpe-activesync/src/tests/stable_sync_does_not_reload_full_email_payloads_without_changes.md)
- [sync_key_stays_usable_for_new_changes_after_a_stable_round](../../../../../functions/crates/lpe-activesync/src/tests/sync_key_stays_usable_for_new_changes_after_a_stable_round.md)
- [stale_sync_key_is_rejected_after_a_completed_round](../../../../../functions/crates/lpe-activesync/src/tests/stale_sync_key_is_rejected_after_a_completed_round.md)
- [superseded_incomplete_sync_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected.md)
- [send_mail_rejects_inaccessible_shared_mailbox_address](../../../../../functions/crates/lpe-activesync/src/tests/send_mail_rejects_inaccessible_shared_mailbox_address.md)
- [item_operations_fetch_returns_attachment_bytes](../../../../../functions/crates/lpe-activesync/src/tests/item_operations_fetch_returns_attachment_bytes.md)
- [search_queries_canonical_mail_projection](../../../../../functions/crates/lpe-activesync/src/tests/search_queries_canonical_mail_projection.md)
- [ping_reports_changed_collections_after_sync_state_exists](../../../../../functions/crates/lpe-activesync/src/tests/ping_reports_changed_collections_after_sync_state_exists.md)
- [ping_rejects_unsynchronized_folders](../../../../../functions/crates/lpe-activesync/src/tests/ping_rejects_unsynchronized_folders.md)
- [smart_reply_uses_source_recipients_and_canonical_submission](../../../../../functions/crates/lpe-activesync/src/tests/smart_reply_uses_source_recipients_and_canonical_submission.md)