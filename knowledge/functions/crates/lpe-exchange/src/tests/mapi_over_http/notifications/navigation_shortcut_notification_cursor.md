---
type: Rust Function
title: navigation_shortcut_notification_cursor
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L140-L166
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
---

# Signature

`async fn navigation_shortcut_notification_cursor( storage: &Storage, account_id: Uuid, shortcut_id: Uuid, change_kind: &str, after_cursor: i64, ) -> anyhow::Result<i64>`

# Calls

- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)

# Called by

- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)