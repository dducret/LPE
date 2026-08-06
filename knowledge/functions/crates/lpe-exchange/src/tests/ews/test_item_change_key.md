---
type: Rust Function
title: test_item_change_key
resource: crates/lpe-exchange/src/tests/ews.rs#L5222-L5230
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/folder_change_keys_follow_canonical_revisions_and_reject_stale_updates
  - functions/crates/lpe-exchange/src/tests/ews/create_item_saveonly_stores_message_as_canonical_draft
  - functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor
  - functions/crates/lpe-exchange/src/tests/ews/create_delete_contact_round_trips_through_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync
  - functions/crates/lpe-exchange/src/tests/ews/update_contact_round_trips_through_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/update_contact_unmapped_field_still_advances_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/create_delete_calendar_item_round_trips_through_sync_folder_items
  - functions/crates/lpe-exchange/src/tests/ews/ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items
---

# Signature

`fn test_item_change_key(xml: &str, item_id: &str) -> String`

# Called by

- [folder_change_keys_follow_canonical_revisions_and_reject_stale_updates](../../../../../../functions/crates/lpe-exchange/src/tests/ews/folder_change_keys_follow_canonical_revisions_and_reject_stale_updates.md)
- [create_item_saveonly_stores_message_as_canonical_draft](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_item_saveonly_stores_message_as_canonical_draft.md)
- [pull_and_streaming_notifications_replay_canonical_sql_change_cursor](../../../../../../functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor.md)
- [create_delete_contact_round_trips_through_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_delete_contact_round_trips_through_sync_folder_items.md)
- [ews_contact_change_key_is_stable_across_get_find_and_sync](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync.md)
- [update_contact_round_trips_through_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/update_contact_round_trips_through_sync_folder_items.md)
- [update_contact_unmapped_field_still_advances_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/update_contact_unmapped_field_still_advances_sync_folder_items.md)
- [create_delete_calendar_item_round_trips_through_sync_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_delete_calendar_item_round_trips_through_sync_folder_items.md)
- [ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership.md)
- [sync_folder_items_reports_custom_mailbox_create_and_delete_changes](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes.md)
- [sync_folder_items_reports_public_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items.md)