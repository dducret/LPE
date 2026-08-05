---
type: Rust Method
title: fetch_search_folders
resource: crates/lpe-storage/src/search_folders.rs#L147-L177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_search_folders( &self, account_id: Uuid, ) -> Result<Vec<SearchFolderDefinition>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)