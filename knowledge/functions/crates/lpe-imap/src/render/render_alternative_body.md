---
type: Rust Function
title: render_alternative_body
resource: crates/lpe-imap/src/render.rs#L576-L602
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/multipart_boundary
  called_by:
  - functions/crates/lpe-imap/src/render/render_message_body
  - functions/crates/lpe-imap/src/render/render_mixed_body
---

# Signature

`fn render_alternative_body(email: &ImapEmail) -> String`

# Calls

- [multipart_boundary](../../../../../functions/crates/lpe-imap/src/render/multipart_boundary.md)

# Called by

- [render_message_body](../../../../../functions/crates/lpe-imap/src/render/render_message_body.md)
- [render_mixed_body](../../../../../functions/crates/lpe-imap/src/render/render_mixed_body.md)