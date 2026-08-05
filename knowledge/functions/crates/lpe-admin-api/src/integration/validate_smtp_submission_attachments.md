---
type: Rust Function
title: validate_smtp_submission_attachments
resource: crates/lpe-admin-api/src/integration.rs#L458-L479
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
---

# Signature

`fn validate_smtp_submission_attachments(raw_message: &[u8]) -> anyhow::Result<()>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)