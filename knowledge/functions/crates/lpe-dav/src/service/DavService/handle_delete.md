---
type: Rust Method
title: handle_delete
resource: crates/lpe-dav/src/service.rs#L387-L418
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/resource_id_for_contact_path
  - functions/crates/lpe-dav/src/service/DavService/contact_for_path
  - functions/crates/lpe-dav/src/preconditions/check_delete_preconditions
  - functions/crates/lpe-dav/src/responses/status_only
  - functions/crates/lpe-dav/src/paths/resource_id_for_task_path
  - functions/crates/lpe-dav/src/service/DavService/task_for_path
  - functions/crates/lpe-dav/src/paths/resource_id_for_event_path
  - functions/crates/lpe-dav/src/service/DavService/event_for_path
---

# Signature

`async fn handle_delete( &self, principal: &AccountPrincipal, path: &str, headers: &HeaderMap, ) -> Result<Response>`

# Calls

- [resource_id_for_contact_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_contact_path.md)
- [contact_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/contact_for_path.md)
- [check_delete_preconditions](../../../../../../functions/crates/lpe-dav/src/preconditions/check_delete_preconditions.md)
- [status_only](../../../../../../functions/crates/lpe-dav/src/responses/status_only.md)
- [resource_id_for_task_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_task_path.md)
- [task_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/task_for_path.md)
- [resource_id_for_event_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_event_path.md)
- [event_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)