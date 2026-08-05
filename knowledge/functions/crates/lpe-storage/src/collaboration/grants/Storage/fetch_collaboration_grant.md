---
type: Rust Method
title: fetch_collaboration_grant
resource: crates/lpe-storage/src/collaboration/grants.rs#L573-L699
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
---

# Signature

`pub async fn fetch_collaboration_grant( &self, kind: CollaborationResourceKind, owner_account_id: Uuid, grantee_account_id: Uuid, ) -> Result<Option<CollaborationGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [upsert_collaboration_grant](../../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)