---
type: Rust Function
title: render_message_body
resource: crates/lpe-imap/src/render.rs#L512-L520
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/email_has_attachment_parts
  - functions/crates/lpe-imap/src/render/render_mixed_body
  - functions/crates/lpe-imap/src/render/render_alternative_body
  called_by:
  - functions/crates/lpe-imap/src/render/append_body_section
---

# Signature

`fn render_message_body(email: &ImapEmail) -> String`

# Calls

- [email_has_attachment_parts](../../../../../functions/crates/lpe-imap/src/render/email_has_attachment_parts.md)
- [render_mixed_body](../../../../../functions/crates/lpe-imap/src/render/render_mixed_body.md)
- [render_alternative_body](../../../../../functions/crates/lpe-imap/src/render/render_alternative_body.md)

# Called by

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)