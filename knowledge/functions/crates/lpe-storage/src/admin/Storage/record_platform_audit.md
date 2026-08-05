---
type: Rust Method
title: record_platform_audit
resource: crates/lpe-storage/src/admin.rs#L27-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
---

# Signature

`pub async fn record_platform_audit(&self, audit: AuditEntryInput) -> Result<()>`

# Calls

- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)

# Called by

- [create_snapshot](../../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [delete_snapshot](../../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [restore_snapshot](../../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)