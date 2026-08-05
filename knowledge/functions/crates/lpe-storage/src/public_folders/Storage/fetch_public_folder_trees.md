---
type: Rust Method
title: fetch_public_folder_trees
resource: crates/lpe-storage/src/public_folders.rs#L185-L221
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_public_folder_trees( &self, account_id: Uuid, ) -> Result<Vec<PublicFolderTree>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)