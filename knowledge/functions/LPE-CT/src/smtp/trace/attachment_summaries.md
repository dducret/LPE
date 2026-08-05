---
type: Rust Function
title: attachment_summaries
resource: LPE-CT/src/smtp/trace.rs#L201-L217
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
---

# Signature

`fn attachment_summaries(data: &[u8]) -> Vec<TraceAttachmentSummary>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)