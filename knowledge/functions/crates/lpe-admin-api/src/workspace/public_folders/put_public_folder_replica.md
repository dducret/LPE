---
type: Rust Function
title: put_public_folder_replica
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L339-L364
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica
---

# Signature

`pub(crate) async fn put_public_folder_replica( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, Json(request): Json<PublicFolderReplicaRequest>, ) -> ApiResult<PublicFolderReplica>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [upsert_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)