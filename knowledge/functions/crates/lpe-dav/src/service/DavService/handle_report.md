---
type: Rust Method
title: handle_report
resource: crates/lpe-dav/src/service.rs#L244-L280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/report/parse_report_filter
  - functions/crates/lpe-dav/src/paths/collection_id_from_contact_path
  - functions/crates/lpe-dav/src/report/contact_matches_report
  - functions/crates/lpe-dav/src/paths/task_collection_id_from_path
  - functions/crates/lpe-dav/src/report/task_matches_report
  - functions/crates/lpe-dav/src/paths/collection_id_from_event_path
  - functions/crates/lpe-dav/src/report/event_matches_report
  - functions/crates/lpe-dav/src/responses/multistatus_response
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle
---

# Signature

`async fn handle_report( &self, principal: &AccountPrincipal, path: &str, body: &[u8], ) -> Result<Response>`

# Calls

- [parse_report_filter](../../../../../../functions/crates/lpe-dav/src/report/parse_report_filter.md)
- [collection_id_from_contact_path](../../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_contact_path.md)
- [contact_matches_report](../../../../../../functions/crates/lpe-dav/src/report/contact_matches_report.md)
- [task_collection_id_from_path](../../../../../../functions/crates/lpe-dav/src/paths/task_collection_id_from_path.md)
- [task_matches_report](../../../../../../functions/crates/lpe-dav/src/report/task_matches_report.md)
- [collection_id_from_event_path](../../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_event_path.md)
- [event_matches_report](../../../../../../functions/crates/lpe-dav/src/report/event_matches_report.md)
- [multistatus_response](../../../../../../functions/crates/lpe-dav/src/responses/multistatus_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)