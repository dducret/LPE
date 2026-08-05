---
type: Rust Function
title: alternative_boundary
resource: crates/lpe-exchange/src/service/ews/mime.rs#L160-L162
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_standalone_body_mime
  - functions/crates/lpe-exchange/src/service/ews/mime/render_body_mime_part
---

# Signature

`pub(in crate::service) fn alternative_boundary(email: &JmapEmail) -> String`

# Called by

- [render_standalone_body_mime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_standalone_body_mime.md)
- [render_body_mime_part](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_body_mime_part.md)