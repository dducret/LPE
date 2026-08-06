---
type: Rust Function
title: test_xml_text
resource: crates/lpe-exchange/src/tests/ews.rs#L5213-L5220
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_hierarchy_replays_canonical_mailbox_changes
  - functions/crates/lpe-exchange/src/tests/ews/pull_subscription_get_events_replays_canonical_changes_after_restart
  - functions/crates/lpe-exchange/src/tests/ews/pull_subscription_get_events_replays_canonical_delete
  - functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor
  - functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_pages_more_than_two_hundred_mailbox_messages
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_honors_max_changes_returned
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items
---

# Signature

`fn test_xml_text(xml: &str, local_name: &str) -> Option<String>`

# Called by

- [sync_folder_hierarchy_replays_canonical_mailbox_changes](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_hierarchy_replays_canonical_mailbox_changes.md)
- [pull_subscription_get_events_replays_canonical_changes_after_restart](../../../../../../functions/crates/lpe-exchange/src/tests/ews/pull_subscription_get_events_replays_canonical_changes_after_restart.md)
- [pull_subscription_get_events_replays_canonical_delete](../../../../../../functions/crates/lpe-exchange/src/tests/ews/pull_subscription_get_events_replays_canonical_delete.md)
- [pull_and_streaming_notifications_replay_canonical_sql_change_cursor](../../../../../../functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor.md)
- [ews_contact_change_key_is_stable_across_get_find_and_sync](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync.md)
- [sync_folder_items_reports_custom_mailbox_create_and_delete_changes](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes.md)
- [sync_folder_items_pages_more_than_two_hundred_mailbox_messages](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_pages_more_than_two_hundred_mailbox_messages.md)
- [sync_folder_items_honors_max_changes_returned](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_honors_max_changes_returned.md)
- [sync_folder_items_reports_public_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items.md)