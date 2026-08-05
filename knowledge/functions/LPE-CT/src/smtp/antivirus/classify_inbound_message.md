---
type: Rust Function
title: classify_inbound_message
resource: LPE-CT/src/smtp/antivirus.rs#L128-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/inbound_mismatch_is_rejected_before_delivery
---

# Signature

`pub(crate) fn classify_inbound_message<D: Detector>( validator: &Validator<D>, message_bytes: &[u8], ) -> Result<InboundMagikaOutcome>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [inbound_mismatch_is_rejected_before_delivery](../../../../../functions/LPE-CT/src/smtp/tests/inbound_mismatch_is_rejected_before_delivery.md)