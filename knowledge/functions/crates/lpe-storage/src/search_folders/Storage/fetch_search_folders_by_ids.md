---
type: Rust Method
title: fetch_search_folders_by_ids
resource: crates/lpe-storage/src/search_folders.rs#L179-L215
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_search_folders_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<SearchFolderDefinition>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)