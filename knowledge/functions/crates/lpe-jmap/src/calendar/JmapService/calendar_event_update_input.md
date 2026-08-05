---
type: Rust Method
title: calendar_event_update_input
resource: crates/lpe-jmap/src/calendar.rs#L602-L637
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/has_jmap_property_patch
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_attachments_by_event
  - functions/crates/lpe-jmap/src/calendar/calendar_event_to_value
  - functions/crates/lpe-jmap/src/calendar/calendar_event_properties
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
---

# Signature

`async fn calendar_event_update_input( &self, account_id: Uuid, event_id: Uuid, value: Value, ) -> Result<(UpsertClientEventInput, Vec<CalendarAttachmentInput>)>`

# Calls

- [has_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/has_jmap_property_patch.md)
- [parse_calendar_event_input](../../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [calendar_attachments_by_event](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_attachments_by_event.md)
- [calendar_event_to_value](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)
- [calendar_event_properties](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_properties.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [apply_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch.md)

# Called by

- [handle_calendar_event_set](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)