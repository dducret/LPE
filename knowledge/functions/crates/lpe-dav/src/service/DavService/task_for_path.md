---
type: Rust Method
title: task_for_path
resource: crates/lpe-dav/src/service.rs#L452-L462
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/resource_id_for_task_path
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_put
  - functions/crates/lpe-dav/src/service/DavService/handle_delete
---

# Signature

`async fn task_for_path(&self, account_id: Uuid, path: &str) -> Result<Option<DavTask>>`

# Calls

- [resource_id_for_task_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_task_path.md)

# Called by

- [handle_propfind](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_get](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
- [handle_delete](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)