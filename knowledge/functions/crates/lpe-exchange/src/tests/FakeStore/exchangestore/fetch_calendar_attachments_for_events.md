---
type: Rust Method
title: fetch_calendar_attachments_for_events
resource: crates/lpe-exchange/src/tests/mod.rs#L11183-L11199
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_calendar_attachments_for_events<'a>( &'a self, _account_id: Uuid, event_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<(Uuid, Vec<CalendarEventAttachment>)>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)