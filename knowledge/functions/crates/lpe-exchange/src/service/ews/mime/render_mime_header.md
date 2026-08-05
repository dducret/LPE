---
type: Rust Function
title: render_mime_header
resource: crates/lpe-exchange/src/service/ews/mime.rs#L52-L89
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/mime/body_content_type
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message
---

# Signature

`pub(in crate::service) fn render_mime_header( email: &JmapEmail, without_attachments: bool, ) -> String`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [body_content_type](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/body_content_type.md)

# Called by

- [render_mime_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message.md)