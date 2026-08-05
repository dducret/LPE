---
type: Rust Method
title: upsert_search_folder
resource: crates/lpe-storage/src/search_folders.rs#L217-L391
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/search_folders/validate_search_folder_input
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  - functions/crates/lpe-storage/src/search_folders/map_search_folder
---

# Signature

`pub async fn upsert_search_folder( &self, input: UpsertSearchFolderInput, ) -> Result<SearchFolderDefinition>`

# Calls

- [validate_search_folder_input](../../../../../../functions/crates/lpe-storage/src/search_folders/validate_search_folder_input.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [map_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/map_search_folder.md)