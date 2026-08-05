---
type: Rust Method
title: handle_put
resource: crates/lpe-dav/src/service.rs#L324-L385
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/resource_id_for_contact_path
  - functions/crates/lpe-dav/src/service/DavService/contact_for_path
  - functions/crates/lpe-dav/src/preconditions/check_write_preconditions
  - functions/crates/lpe-dav/src/parse/parse_vcard
  - functions/crates/lpe-dav/src/responses/status_with_etag
  - functions/crates/lpe-dav/src/paths/etag_for_contact
  - functions/crates/lpe-dav/src/paths/resource_id_for_task_path
  - functions/crates/lpe-dav/src/service/DavService/task_for_path
  - functions/crates/lpe-dav/src/parse/parse_vtodo
  - functions/crates/lpe-dav/src/paths/etag_for_task
  - functions/crates/lpe-dav/src/paths/resource_id_for_event_path
  - functions/crates/lpe-dav/src/service/DavService/event_for_path
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/paths/etag_for_event
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle
---

# Signature

`async fn handle_put( &self, principal: &AccountPrincipal, path: &str, headers: &HeaderMap, body: &[u8], ) -> Result<Response>`

# Calls

- [resource_id_for_contact_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_contact_path.md)
- [contact_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/contact_for_path.md)
- [check_write_preconditions](../../../../../../functions/crates/lpe-dav/src/preconditions/check_write_preconditions.md)
- [parse_vcard](../../../../../../functions/crates/lpe-dav/src/parse/parse_vcard.md)
- [status_with_etag](../../../../../../functions/crates/lpe-dav/src/responses/status_with_etag.md)
- [etag_for_contact](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_contact.md)
- [resource_id_for_task_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_task_path.md)
- [task_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/task_for_path.md)
- [parse_vtodo](../../../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)
- [etag_for_task](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_task.md)
- [resource_id_for_event_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_event_path.md)
- [event_for_path](../../../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)
- [parse_ical](../../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [etag_for_event](../../../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)