---
type: Rust Function
title: delete_snapshot
resource: crates/lpe-admin-api/src/snapshots.rs#L112-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/snapshots/load_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_dir
  - functions/crates/lpe-admin-api/src/snapshots/remove_if_exists
  - functions/crates/lpe-storage/src/admin/Storage/record_platform_audit
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
---

# Signature

`pub(crate) async fn delete_snapshot( State(storage): State<Storage>, headers: HeaderMap, AxumPath(snapshot_id): AxumPath<String>, ) -> ApiResult<SnapshotListResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [load_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/load_snapshot.md)
- [snapshot_dir](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_dir.md)
- [remove_if_exists](../../../../../functions/crates/lpe-admin-api/src/snapshots/remove_if_exists.md)
- [record_platform_audit](../../../../../functions/crates/lpe-storage/src/admin/Storage/record_platform_audit.md)
- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)