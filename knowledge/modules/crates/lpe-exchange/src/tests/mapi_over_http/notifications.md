---
type: Rust Module
title: notifications
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1-L1950
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/sqlx-row
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [notification_event_input](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/notification_event_input.md)
- [insert_notification_account](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account.md)
- [calendar_notification_ids](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids.md)
- [assert_calendar_notification](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_calendar_notification.md)
- [assert_navigation_shortcut_notification](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_navigation_shortcut_notification.md)
- [navigation_shortcut_notification_cursor](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/navigation_shortcut_notification_cursor.md)
- [assert_outsider_has_no_notifications](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_outsider_has_no_notifications.md)
- [mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql.md)
- [mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql.md)
- [mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql.md)
- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)
- [mapi_contact_notification_create_carries_current_total_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_contact_notification_create_carries_current_total_in_postgresql.md)
- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)
- [create_notification_mailbox](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)

# Imports

- `super::*`
- `sqlx::Row`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)