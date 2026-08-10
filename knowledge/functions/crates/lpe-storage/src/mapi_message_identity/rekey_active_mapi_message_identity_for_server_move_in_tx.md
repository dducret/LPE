---
type: Rust Function
title: rekey_active_mapi_message_identity_for_server_move_in_tx
resource: crates/lpe-storage/src/mapi_message_identity.rs#L89-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
---

# Signature

`pub(crate) async fn rekey_active_mapi_message_identity_for_server_move_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, ) -> Result<Option<MapiMessageIdentityMove>>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [mapi_xid](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid.md)
- [mapi_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [move_jmap_email_membership](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)