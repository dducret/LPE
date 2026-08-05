---
type: Rust Function
title: validate_mime_attachments_with_validator
resource: crates/lpe-activesync/src/service/mime_validation.rs#L10-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments
  - functions/crates/lpe-activesync/src/service/mime_validation/activesync_sendmail_blocks_mismatched_attachment_payloads
---

# Signature

`pub(super) fn validate_mime_attachments_with_validator<D: Detector>( validator: &Validator<D>, bytes: &[u8], ) -> Result<()>`

# Calls

- [collect_mime_attachment_parts](../../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [validate_mime_attachments](../../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments.md)
- [activesync_sendmail_blocks_mismatched_attachment_payloads](../../../../../../functions/crates/lpe-activesync/src/service/mime_validation/activesync_sendmail_blocks_mismatched_attachment_payloads.md)