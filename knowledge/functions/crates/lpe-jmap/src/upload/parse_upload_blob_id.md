---
type: Rust Function
title: parse_upload_blob_id
resource: crates/lpe-jmap/src/upload.rs#L20-L27
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
---

# Signature

`pub(crate) fn parse_upload_blob_id(value: &str) -> Result<Uuid>`

# Called by

- [parse_calendar_attachment_inputs](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs.md)
- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)