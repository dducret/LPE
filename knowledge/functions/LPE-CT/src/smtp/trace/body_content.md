---
type: Rust Function
title: body_content
resource: LPE-CT/src/smtp/trace.rs#L193-L199
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/extract_visible_text
  - functions/LPE-CT/src/smtp/trace/body_excerpt
  called_by:
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
---

# Signature

`fn body_content(data: &[u8]) -> String`

# Calls

- [extract_visible_text](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [body_excerpt](../../../../../functions/LPE-CT/src/smtp/trace/body_excerpt.md)

# Called by

- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)