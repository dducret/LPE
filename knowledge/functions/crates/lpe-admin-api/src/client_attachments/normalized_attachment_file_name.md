---
type: Rust Function
title: normalized_attachment_file_name
resource: crates/lpe-admin-api/src/client_attachments.rs#L212-L216
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/read_multipart_attachment
  - functions/crates/lpe-admin-api/src/client_attachments/attachment_content_response
---

# Signature

`fn normalized_attachment_file_name(file_name: Option<&str>) -> Option<String>`

# Called by

- [read_multipart_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/read_multipart_attachment.md)
- [attachment_content_response](../../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_content_response.md)