---
type: Rust Function
title: parse_calendar_attachment_inputs
resource: crates/lpe-jmap/src/calendar.rs#L1225-L1250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/upload/parse_upload_blob_id
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn parse_calendar_attachment_inputs(value: Option<&Value>) -> Result<Vec<CalendarAttachmentInput>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_upload_blob_id](../../../../../functions/crates/lpe-jmap/src/upload/parse_upload_blob_id.md)
- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)