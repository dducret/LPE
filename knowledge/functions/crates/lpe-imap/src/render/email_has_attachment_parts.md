---
type: Rust Function
title: email_has_attachment_parts
resource: crates/lpe-imap/src/render.rs#L788-L790
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/render_message_body
  - functions/crates/lpe-imap/src/render/root_content_type
---

# Signature

`fn email_has_attachment_parts(email: &ImapEmail) -> bool`

# Called by

- [render_message_body](../../../../../functions/crates/lpe-imap/src/render/render_message_body.md)
- [root_content_type](../../../../../functions/crates/lpe-imap/src/render/root_content_type.md)