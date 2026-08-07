---
type: Rust Method
title: fetch_calendar_event_attachments
resource: crates/lpe-storage/src/attachments.rs#L358-L393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event
---

# Signature

`pub async fn fetch_calendar_event_attachments( &self, account_id: Uuid, event_id: Uuid, ) -> Result<Vec<CalendarEventAttachment>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event.md)