---
type: Rust Function
title: list_snapshots
resource: crates/lpe-admin-api/src/snapshots.rs#L47-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
---

# Signature

`pub(crate) async fn list_snapshots( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<SnapshotListResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)