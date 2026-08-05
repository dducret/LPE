---
type: Rust Method
title: handle_propfind
resource: crates/lpe-dav/src/service.rs#L108-L242
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-dav/src/service/DavService/contact_for_path
  - functions/crates/lpe-dav/src/service/DavService/task_for_path
  - functions/crates/lpe-dav/src/service/DavService/event_for_path
  - functions/crates/lpe-dav/src/paths/collection_id_from_contact_path
  - functions/crates/lpe-dav/src/paths/task_collection_id_from_path
  - functions/crates/lpe-dav/src/paths/collection_id_from_event_path
  - functions/crates/lpe-dav/src/responses/multistatus_response
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle
---

# Signature

`async fn handle_propfind( &self, principal: &AccountPrincipal, path: &str, headers: &HeaderMap, ) -> Result<Response>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [contact_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/contact_for_path.md)
- [task_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/task_for_path.md)
- [event_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)
- [collection_id_from_contact_path](../../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_contact_path.md)
- [task_collection_id_from_path](../../../../../../functions/crates/lpe-dav/src/paths/task_collection_id_from_path.md)
- [collection_id_from_event_path](../../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_event_path.md)
- [multistatus_response](../../../../../../functions/crates/lpe-dav/src/responses/multistatus_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)