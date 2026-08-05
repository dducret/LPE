---
type: Rust Function
title: render_attachment_mime_part
resource: crates/lpe-exchange/src/service/ews/mime.rs#L127-L143
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mime/quote_mime_parameter
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message
---

# Signature

`pub(in crate::service) fn render_attachment_mime_part( attachment: &ActiveSyncAttachmentContent, ) -> String`

# Calls

- [quote_mime_parameter](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/quote_mime_parameter.md)

# Called by

- [render_mime_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message.md)