---
type: Rust Method
title: fetch_calendar_attachments_for_events
resource: crates/lpe-storage/src/attachments.rs#L368-L419
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/attachments/calendar_event_attachment_from_row
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`pub async fn fetch_calendar_attachments_for_events( &self, account_id: Uuid, event_ids: &[Uuid], ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [calendar_event_attachment_from_row](../../../../../../functions/crates/lpe-storage/src/attachments/calendar_event_attachment_from_row.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)