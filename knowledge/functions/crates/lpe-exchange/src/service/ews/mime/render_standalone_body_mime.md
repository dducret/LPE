---
type: Rust Function
title: render_standalone_body_mime
resource: crates/lpe-exchange/src/service/ews/mime.rs#L26-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mime/alternative_boundary
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message
---

# Signature

`pub(in crate::service) fn render_standalone_body_mime(email: &JmapEmail) -> String`

# Calls

- [alternative_boundary](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/alternative_boundary.md)

# Called by

- [render_mime_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_message.md)