---
type: Rust Function
title: render_mime_address
resource: crates/lpe-exchange/src/service/ews/mime.rs#L172-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/format_mailbox_address
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_recipients
---

# Signature

`pub(in crate::service) fn render_mime_address(display_name: Option<&str>, address: &str) -> String`

# Calls

- [format_mailbox_address](../../../../../../../functions/crates/lpe-domain/src/mail_format/format_mailbox_address.md)

# Called by

- [render_mime_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_recipients.md)