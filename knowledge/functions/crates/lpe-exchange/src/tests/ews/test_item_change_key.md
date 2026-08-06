---
type: Rust Function
title: test_item_change_key
resource: crates/lpe-exchange/src/tests/ews.rs#L4889-L4897
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/create_item_saveonly_stores_message_as_canonical_draft
  - functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor
  - functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync
  - functions/crates/lpe-exchange/src/tests/ews/ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items
---

# Signature

`fn test_item_change_key(xml: &str, item_id: &str) -> String`

# Called by

- [create_item_saveonly_stores_message_as_canonical_draft](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_item_saveonly_stores_message_as_canonical_draft.md)
- [pull_and_streaming_notifications_replay_canonical_sql_change_cursor](../../../../../../functions/crates/lpe-exchange/src/tests/ews/pull_and_streaming_notifications_replay_canonical_sql_change_cursor.md)
- [ews_contact_change_key_is_stable_across_get_find_and_sync](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_contact_change_key_is_stable_across_get_find_and_sync.md)
- [ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership](../../../../../../functions/crates/lpe-exchange/src/tests/ews/ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership.md)
- [sync_folder_items_reports_custom_mailbox_create_and_delete_changes](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_custom_mailbox_create_and_delete_changes.md)
- [sync_folder_items_reports_public_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_reports_public_folder_items.md)