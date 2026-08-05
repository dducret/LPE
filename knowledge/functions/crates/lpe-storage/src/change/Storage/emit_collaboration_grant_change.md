---
type: Rust Method
title: emit_collaboration_grant_change
resource: crates/lpe-storage/src/change.rs#L376-L408
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant
---

# Signature

`pub(crate) async fn emit_collaboration_grant_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, kind: CollaborationResourceKind, owner_account_id: Uuid, grantee_account_id: Uuid, ) -> Result<()>`

# Calls

- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [delete_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant.md)
- [set_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant.md)