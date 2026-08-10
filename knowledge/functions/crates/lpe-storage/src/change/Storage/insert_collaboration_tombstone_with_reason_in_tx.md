---
type: Rust Method
title: insert_collaboration_tombstone_with_reason_in_tx
resource: crates/lpe-storage/src/change.rs#L515-L571
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_move_tombstone_in_tx
---

# Signature

`async fn insert_collaboration_tombstone_with_reason_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, owner_account_id: Uuid, collection_id: Option<Uuid>, object_kind: &str, object_id: Uuid, object_uid: Option<&str>, affected_principal_ids: &[Uuid], reason: &str, ) -> Result<()>`

# Calls

- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [insert_collaboration_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [insert_collaboration_move_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_move_tombstone_in_tx.md)