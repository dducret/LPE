---
type: Rust Function
title: body_excerpt
resource: LPE-CT/src/smtp/trace.rs#L175-L191
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/trace/body_content
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
---

# Signature

`fn body_excerpt(data: &[u8]) -> String`

# Called by

- [body_content](../../../../../functions/LPE-CT/src/smtp/trace/body_content.md)
- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)