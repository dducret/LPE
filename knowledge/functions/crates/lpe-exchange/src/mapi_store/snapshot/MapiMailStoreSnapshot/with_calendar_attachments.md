---
type: Rust Method
title: with_calendar_attachments
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L709-L722
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_calendar_attachments( mut self, calendar_attachments: Vec<(Uuid, Vec<CalendarEventAttachment>)>, ) -> Self`

# Calls

- [calendar_mapi_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)