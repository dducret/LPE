---
type: Rust Function
title: calendar_mapi_attachments
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L7-L22
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_updated_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_calendar_attachments
---

# Signature

`fn calendar_mapi_attachments(attachments: &[CalendarEventAttachment]) -> Vec<MapiAttachment>`

# Called by

- [remember_created_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event.md)
- [remember_updated_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_updated_event.md)
- [with_calendar_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_calendar_attachments.md)