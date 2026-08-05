---
type: Rust Method
title: insert_collaboration_move_tombstone_in_tx
resource: crates/lpe-storage/src/change.rs#L488-L513
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx
  called_by:
  - functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items
---

# Signature

`pub(crate) async fn insert_collaboration_move_tombstone_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, owner_account_id: Uuid, collection_id: Option<Uuid>, object_kind: &str, object_id: Uuid, object_uid: Option<&str>, affected_principal_ids: &[Uuid], ) -> Result<()>`

# Calls

- [insert_collaboration_tombstone_with_reason_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx.md)

# Called by

- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items.md)