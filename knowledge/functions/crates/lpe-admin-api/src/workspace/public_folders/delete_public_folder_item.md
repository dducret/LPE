---
type: Rust Function
title: delete_public_folder_item
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L231-L254
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_public_folder_item( State(storage): State<Storage>, headers: HeaderMap, AxumPath((folder_id, item_id)): AxumPath<(Uuid, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)