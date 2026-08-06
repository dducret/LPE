---
type: Rust Method
title: fetch_mapi_notification_cursor
resource: crates/lpe-exchange/src/tests/mod.rs#L7440-L7446
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_contact_notification_create_carries_current_total_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
---

# Signature

`fn fetch_mapi_notification_cursor<'a>( &'a self, _account_id: Uuid, ) -> StoreFuture<'a, Option<i64>>`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [register_pull_subscription](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription.md)
- [mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql.md)
- [mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql.md)
- [mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql.md)
- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)
- [mapi_contact_notification_create_carries_current_total_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_contact_notification_create_carries_current_total_in_postgresql.md)
- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)