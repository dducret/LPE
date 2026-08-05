---
type: Rust Function
title: body_part_charset
resource: crates/lpe-imap/src/render.rs#L988-L1002
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/render_body_bodystructure
  - functions/crates/lpe-imap/src/render/render_part_mime_header
---

# Signature

`fn body_part_charset(email: &ImapEmail, path: &str, fallback: &str) -> String`

# Called by

- [render_body_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_body_bodystructure.md)
- [render_part_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_part_mime_header.md)