---
type: Rust Function
title: validate_client_attachment_with_validator
resource: crates/lpe-admin-api/src/client_attachments.rs#L190-L210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment
---

# Signature

`fn validate_client_attachment_with_validator<D: Detector>( validator: &Validator<D>, attachment: AttachmentUploadInput, ) -> anyhow::Result<AttachmentUploadInput>`

# Called by

- [upload_draft_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment.md)