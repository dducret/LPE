---
type: Rust Function
title: encode_quoted_printable
resource: LPE-CT/src/smtp/outbound.rs#L98-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/outbound/compose_rfc822_message
  - functions/LPE-CT/src/smtp/tests/quoted_printable_encoder_handles_utf8_and_line_breaks
---

# Signature

`pub(crate) fn encode_quoted_printable(value: &str) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [compose_rfc822_message](../../../../../functions/LPE-CT/src/smtp/outbound/compose_rfc822_message.md)
- [quoted_printable_encoder_handles_utf8_and_line_breaks](../../../../../functions/LPE-CT/src/smtp/tests/quoted_printable_encoder_handles_utf8_and_line_breaks.md)