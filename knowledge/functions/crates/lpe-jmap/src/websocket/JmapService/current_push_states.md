---
type: Rust Method
title: current_push_states
resource: crates/lpe-jmap/src/websocket.rs#L649-L689
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/tests/websocket_push_states_include_shared_mailbox_accounts
  - functions/crates/lpe-jmap/src/tests/websocket_push_states_include_submission_identity_mail_types
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_is_stable_for_noop_mail_notifications
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_limits_recompute_to_requested_categories
  - functions/crates/lpe-jmap/src/tests/websocket_push_tracks_private_outlook_note_and_journal_types
  - functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
  - functions/crates/lpe-jmap/src/websocket/JmapService/enable_push
---

# Signature

`pub(crate) async fn current_push_states( &self, principal_account_id: Uuid, data_types: &HashSet<String>, ) -> Result<HashMap<String, HashMap<String, String>>>`

# Calls

- [is_mail_push_type](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type.md)
- [mail_push_type_state](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [websocket_push_states_include_shared_mailbox_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_states_include_shared_mailbox_accounts.md)
- [websocket_push_states_include_submission_identity_mail_types](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_states_include_submission_identity_mail_types.md)
- [websocket_push_enable_sends_full_state_for_missing_or_stale_push_state](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state.md)
- [websocket_push_enable_refreshes_cursor_for_unchanged_states](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states.md)
- [websocket_reconnect_recovers_task_changes_from_canonical_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
- [websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated.md)
- [scoped_push_change_is_stable_for_noop_mail_notifications](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_is_stable_for_noop_mail_notifications.md)
- [scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes.md)
- [scoped_push_change_reports_delegated_mailbox_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes.md)
- [scoped_push_change_reports_delegated_identity_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes.md)
- [scoped_push_change_reports_email_delivery_for_new_messages_only_state](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state.md)
- [scoped_push_change_limits_recompute_to_requested_categories](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_limits_recompute_to_requested_categories.md)
- [websocket_push_tracks_private_outlook_note_and_journal_types](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_tracks_private_outlook_note_and_journal_types.md)
- [shared_task_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal.md)
- [shared_task_list_rights_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal.md)
- [deleted_shared_task_list_push_change_wakes_former_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal.md)
- [benchmark_mailbox_listing_and_push_paths](../../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)
- [enable_push](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)