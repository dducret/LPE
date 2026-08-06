---
type: Rust Function
title: create_notification_mailbox
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1274-L1293
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql
---

# Signature

`async fn create_notification_mailbox( storage: &Storage, account_id: Uuid, name: &str, parent_id: Option<Uuid>, ) -> anyhow::Result<JmapMailbox>`

# Calls

- [postgres_mapi_audit](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)

# Called by

- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)