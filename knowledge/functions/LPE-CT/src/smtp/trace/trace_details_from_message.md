---
type: Rust Function
title: trace_details_from_message
resource: LPE-CT/src/smtp/trace.rs#L219-L255
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/LPE-CT/src/smtp/trace/inspect_headers
  - functions/LPE-CT/src/smtp/trace/body_excerpt
  - functions/LPE-CT/src/smtp/trace/body_content
  - functions/LPE-CT/src/smtp/trace/attachment_summaries
  called_by:
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
---

# Signature

`pub(in crate::smtp) fn trace_details_from_message( queue: &str, message: &QueuedMessage, ) -> TraceDetails`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [inspect_headers](../../../../../functions/LPE-CT/src/smtp/trace/inspect_headers.md)
- [body_excerpt](../../../../../functions/LPE-CT/src/smtp/trace/body_excerpt.md)
- [body_content](../../../../../functions/LPE-CT/src/smtp/trace/body_content.md)
- [attachment_summaries](../../../../../functions/LPE-CT/src/smtp/trace/attachment_summaries.md)

# Called by

- [load_trace_details](../../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)