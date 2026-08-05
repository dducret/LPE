---
type: Rust Method
title: record_public_folder_change_with_extra_affected
resource: crates/lpe-storage/src/public_folders/changes.rs#L34-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
---

# Signature

`pub(super) async fn record_public_folder_change_with_extra_affected( &self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, access: &PublicFolderAccess, actor_account_id: Uuid, folder_id: Uuid, object_kind: &str, object_id: Uuid, change_kind: &str, summary_json: serde_json::Value, extra_affected_account_ids: &[Uuid], ) -> Result<i64>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [allocate_account_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_canonical_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [upsert_public_folder_permission](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission.md)
- [record_public_folder_change](../../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)