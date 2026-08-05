---
type: Rust Function
title: compose_rfc822_message
resource: LPE-CT/src/smtp/outbound.rs#L3-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/outbound/encode_quoted_printable
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_builds_multipart_alternative_when_html_is_present
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_emits_sender_header_for_delegated_sender
---

# Signature

`pub(crate) fn compose_rfc822_message(payload: &OutboundMessageHandoffRequest) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [encode_quoted_printable](../../../../../functions/LPE-CT/src/smtp/outbound/encode_quoted_printable.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [outbound_handoff_builds_multipart_alternative_when_html_is_present](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_builds_multipart_alternative_when_html_is_present.md)
- [outbound_handoff_emits_sender_header_for_delegated_sender](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_emits_sender_header_for_delegated_sender.md)