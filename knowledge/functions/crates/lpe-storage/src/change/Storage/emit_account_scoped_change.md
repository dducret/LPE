---
type: Rust Method
title: emit_account_scoped_change
resource: crates/lpe-storage/src/change.rs#L452-L459
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script
  - functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder
  - functions/crates/lpe-storage/src/search_folders/Storage/delete_search_folder
  - functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders
---

# Signature

`pub(crate) async fn emit_account_scoped_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, account_id: Uuid, ) -> Result<()>`

# Calls

- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [put_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [delete_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script.md)
- [rename_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)
- [upsert_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder.md)
- [delete_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/delete_search_folder.md)
- [ensure_exchange_search_folders](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders.md)