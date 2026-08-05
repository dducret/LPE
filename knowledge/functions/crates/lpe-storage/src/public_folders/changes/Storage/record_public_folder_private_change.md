---
type: Rust Method
title: record_public_folder_private_change
resource: crates/lpe-storage/src/public_folders/changes.rs#L94-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state
---

# Signature

`pub(super) async fn record_public_folder_private_change( &self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, access: &PublicFolderAccess, actor_account_id: Uuid, object_kind: &str, object_id: Uuid, change_kind: &str, summary_json: serde_json::Value, ) -> Result<i64>`

# Calls

- [allocate_account_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_canonical_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [patch_public_folder_per_user_state](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state.md)