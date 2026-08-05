---
type: Rust Function
title: quote_mime_parameter
resource: crates/lpe-exchange/src/service/ews/mime.rs#L180-L182
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/quote_header_parameter
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_attachment_mime_part
---

# Signature

`pub(in crate::service) fn quote_mime_parameter(value: &str) -> String`

# Calls

- [quote_header_parameter](../../../../../../../functions/crates/lpe-domain/src/mail_format/quote_header_parameter.md)

# Called by

- [render_attachment_mime_part](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_attachment_mime_part.md)