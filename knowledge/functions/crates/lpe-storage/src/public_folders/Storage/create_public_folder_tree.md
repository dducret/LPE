---
type: Rust Method
title: create_public_folder_tree
resource: crates/lpe-storage/src/public_folders.rs#L29-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn create_public_folder_tree( &self, input: CreatePublicFolderTreeInput, audit: AuditEntryInput, ) -> Result<PublicFolder>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [record_public_folder_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)