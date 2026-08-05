---
type: Rust Function
title: render_mime_message
resource: crates/lpe-exchange/src/service/ews/mime.rs#L3-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_header
  - functions/crates/lpe-exchange/src/service/ews/mime/render_standalone_body_mime
  - functions/crates/lpe-exchange/src/service/ews/mime/render_body_mime_part
  - functions/crates/lpe-exchange/src/service/ews/mime/render_attachment_mime_part
---

# Signature

`pub(in crate::service) fn render_mime_message( email: &JmapEmail, attachments: &[ActiveSyncAttachmentContent], ) -> Vec<u8>`

# Calls

- [render_mime_header](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_header.md)
- [render_standalone_body_mime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_standalone_body_mime.md)
- [render_body_mime_part](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_body_mime_part.md)
- [render_attachment_mime_part](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_attachment_mime_part.md)