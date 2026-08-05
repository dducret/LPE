---
type: Rust Method
title: attach_calendar_uploads
resource: crates/lpe-jmap/src/calendar.rs#L659-L722
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
---

# Signature

`async fn attach_calendar_uploads( &self, account_id: Uuid, event_id: Uuid, attachments: Vec<CalendarAttachmentInput>, account: &AuthenticatedAccount, ) -> Result<()>`

# Called by

- [handle_calendar_event_set](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)