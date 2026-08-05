---
type: Rust Function
title: create_snapshot
resource: crates/lpe-admin-api/src/snapshots.rs#L55-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_database_url
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_dir
  - functions/crates/lpe-admin-api/src/snapshots/clean_label
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_id
  - functions/crates/lpe-admin-api/src/snapshots/pg_tool
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-storage/src/admin/Storage/record_platform_audit
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
---

# Signature

`pub(crate) async fn create_snapshot( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateSnapshotRequest>, ) -> ApiResult<SnapshotListResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [snapshot_database_url](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_database_url.md)
- [snapshot_dir](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_dir.md)
- [clean_label](../../../../../functions/crates/lpe-admin-api/src/snapshots/clean_label.md)
- [snapshot_id](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_id.md)
- [pg_tool](../../../../../functions/crates/lpe-admin-api/src/snapshots/pg_tool.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [record_platform_audit](../../../../../functions/crates/lpe-storage/src/admin/Storage/record_platform_audit.md)
- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)