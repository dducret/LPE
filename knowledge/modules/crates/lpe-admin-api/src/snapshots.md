---
type: Rust Module
title: snapshots
resource: crates/lpe-admin-api/src/snapshots.rs#L1-L322
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-state-http-headermap-statuscode-json
  - external/lpe-storage-auditentryinput-storage
  - external/serde-deserialize-serialize
  - external/std-fs-path-path-pathbuf-process-command-time-systemtime-unix-epoch
  - external/tokio-task
  - external/uuid-uuid
  - external/crate-http-bad-request-error-internal-error-require-admin-types-apiresult
  - external/super-clean-label-clean-snapshot-id
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [CreateSnapshotRequest](../../../../classes/crates/lpe-admin-api/src/snapshots/CreateSnapshotRequest.md)
- [SnapshotMetadata](../../../../classes/crates/lpe-admin-api/src/snapshots/SnapshotMetadata.md)
- [SnapshotListResponse](../../../../classes/crates/lpe-admin-api/src/snapshots/SnapshotListResponse.md)
- [list_snapshots](../../../../functions/crates/lpe-admin-api/src/snapshots/list_snapshots.md)
- [create_snapshot](../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [delete_snapshot](../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [restore_snapshot](../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)
- [snapshot_database_url](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_database_url.md)
- [snapshot_response](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)
- [read_snapshots](../../../../functions/crates/lpe-admin-api/src/snapshots/read_snapshots.md)
- [load_snapshot](../../../../functions/crates/lpe-admin-api/src/snapshots/load_snapshot.md)
- [clean_snapshot_id](../../../../functions/crates/lpe-admin-api/src/snapshots/clean_snapshot_id.md)
- [snapshot_not_found](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_not_found.md)
- [snapshot_dir](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_dir.md)
- [snapshot_id](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_id.md)
- [unix_timestamp](../../../../functions/crates/lpe-admin-api/src/snapshots/unix_timestamp.md)
- [clean_label](../../../../functions/crates/lpe-admin-api/src/snapshots/clean_label.md)
- [pg_tool](../../../../functions/crates/lpe-admin-api/src/snapshots/pg_tool.md)
- [remove_if_exists](../../../../functions/crates/lpe-admin-api/src/snapshots/remove_if_exists.md)
- [snapshot_ids_reject_path_traversal](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_ids_reject_path_traversal.md)
- [snapshot_labels_have_stable_default](../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_labels_have_stable_default.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_storage::{AuditEntryInput, Storage}`
- `serde::{Deserialize, Serialize}`
- `std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
}`
- `tokio::task`
- `uuid::Uuid`
- `crate::{
    http::{bad_request_error, internal_error},
    require_admin,
    types::ApiResult,
}`
- `super::{clean_label, clean_snapshot_id}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)