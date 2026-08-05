---
type: Rust Method
title: insert_accounts
resource: crates/lpe-storage/src/change.rs#L83-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
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
  - functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change
  - functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes
---

# Signature

`pub fn insert_accounts<I>(&mut self, category: CanonicalChangeCategory, account_ids: I) where I: IntoIterator<Item = Uuid>,`

# Calls

- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [websocket_reconnect_recovers_task_changes_from_canonical_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
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
- [wait_for_change](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change.md)
- [replay_canonical_changes](../../../../../../functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes.md)