---
type: Rust Method
title: delete_mapi_navigation_shortcut
resource: crates/lpe-exchange/src/tests/mod.rs#L10021-L10041
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted
---

# Signature

`fn delete_mapi_navigation_shortcut<'a>( &'a self, _account_id: Uuid, shortcut_id: Uuid, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
- [mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted.md)