---
type: Rust Function
title: restore_snapshot
resource: crates/lpe-admin-api/src/snapshots.rs#L143-L190
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_database_url
  - functions/crates/lpe-admin-api/src/snapshots/load_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/pg_tool
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-storage/src/admin/Storage/record_platform_audit
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
---

# Signature

`pub(crate) async fn restore_snapshot( State(storage): State<Storage>, headers: HeaderMap, AxumPath(snapshot_id): AxumPath<String>, ) -> ApiResult<SnapshotListResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [snapshot_database_url](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_database_url.md)
- [load_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/load_snapshot.md)
- [pg_tool](../../../../../functions/crates/lpe-admin-api/src/snapshots/pg_tool.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [record_platform_audit](../../../../../functions/crates/lpe-storage/src/admin/Storage/record_platform_audit.md)
- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)