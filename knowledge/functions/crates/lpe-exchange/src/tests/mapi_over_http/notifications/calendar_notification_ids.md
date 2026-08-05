---
type: Rust Function
title: calendar_notification_ids
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L62-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
---

# Signature

`async fn calendar_notification_ids( storage: &Storage, account_id: Uuid, collection_id: &str, event_id: Uuid, ) -> anyhow::Result<(u64, u64)>`

# Calls

- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [events_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)

# Called by

- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)