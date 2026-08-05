---
type: Rust Function
title: render_mixed_body
resource: crates/lpe-imap/src/render.rs#L604-L635
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/render_alternative_body
  - functions/crates/lpe-imap/src/render/attachment_parts
  called_by:
  - functions/crates/lpe-imap/src/render/render_message_body
---

# Signature

`fn render_mixed_body(email: &ImapEmail) -> String`

# Calls

- [render_alternative_body](../../../../../functions/crates/lpe-imap/src/render/render_alternative_body.md)
- [attachment_parts](../../../../../functions/crates/lpe-imap/src/render/attachment_parts.md)

# Called by

- [render_message_body](../../../../../functions/crates/lpe-imap/src/render/render_message_body.md)