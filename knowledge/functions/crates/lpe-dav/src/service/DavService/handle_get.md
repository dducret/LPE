---
type: Rust Method
title: handle_get
resource: crates/lpe-dav/src/service.rs#L282-L322
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/service/DavService/contact_for_path
  - functions/crates/lpe-dav/src/serialize/serialize_vcard
  - functions/crates/lpe-dav/src/preconditions/precondition_not_modified
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/responses/status_only
  - functions/crates/lpe-dav/src/responses/text_response
  - functions/crates/lpe-dav/src/paths/etag_for_contact
  - functions/crates/lpe-dav/src/service/DavService/task_for_path
  - functions/crates/lpe-dav/src/serialize/serialize_vtodo
  - functions/crates/lpe-dav/src/paths/etag_for_task
  - functions/crates/lpe-dav/src/service/DavService/event_for_path
  - functions/crates/lpe-dav/src/serialize/serialize_ical
  - functions/crates/lpe-dav/src/paths/etag_for_event
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle
---

# Signature

`async fn handle_get( &self, principal: &AccountPrincipal, path: &str, headers: &HeaderMap, ) -> Result<Response>`

# Calls

- [contact_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/contact_for_path.md)
- [serialize_vcard](../../../../../../functions/crates/lpe-dav/src/serialize/serialize_vcard.md)
- [precondition_not_modified](../../../../../../functions/crates/lpe-dav/src/preconditions/precondition_not_modified.md)
- [etag](../../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [status_only](../../../../../../functions/crates/lpe-dav/src/responses/status_only.md)
- [text_response](../../../../../../functions/crates/lpe-dav/src/responses/text_response.md)
- [etag_for_contact](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_contact.md)
- [task_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/task_for_path.md)
- [serialize_vtodo](../../../../../../functions/crates/lpe-dav/src/serialize/serialize_vtodo.md)
- [etag_for_task](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_task.md)
- [event_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)
- [serialize_ical](../../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)
- [etag_for_event](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)