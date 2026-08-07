---
type: Rust Function
title: read_multipart_attachment
resource: crates/lpe-admin-api/src/client_attachments.rs#L145-L188
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_attachments/normalized_attachment_file_name
  - functions/tools/rca_outlook/http/content_type
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment
---

# Signature

`async fn read_multipart_attachment( multipart: &mut Multipart, ) -> Result<AttachmentUploadInput, (StatusCode, String)>`

# Calls

- [normalized_attachment_file_name](../../../../../functions/crates/lpe-admin-api/src/client_attachments/normalized_attachment_file_name.md)
- [content_type](../../../../../functions/tools/rca_outlook/http/content_type.md)

# Called by

- [upload_draft_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment.md)