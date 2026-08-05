---
type: Rust Function
title: rekey_mapi_message_identity_in_tx
resource: crates/lpe-storage/src/message_ops.rs#L1334-L1468
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/message_ops/imported_message_move_destination_global_counter
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  called_by:
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
---

# Signature

`async fn rekey_mapi_message_identity_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, imported_identity: &MapiMessageImportedMoveIdentity, ) -> Result<MapiMessageIdentityMove>`

# Calls

- [imported_message_move_destination_global_counter](../../../../../functions/crates/lpe-storage/src/message_ops/imported_message_move_destination_global_counter.md)
- [ensure_mapi_store_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)

# Called by

- [move_jmap_email_membership](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)