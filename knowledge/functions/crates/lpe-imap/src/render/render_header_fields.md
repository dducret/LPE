---
type: Rust Function
title: render_header_fields
resource: crates/lpe-imap/src/render.rs#L466-L502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/render_header_lines
  called_by:
  - functions/crates/lpe-imap/src/render/append_body_section
---

# Signature

`fn render_header_fields(email: &ImapEmail, section: &str) -> String`

# Calls

- [render_header_lines](../../../../../functions/crates/lpe-imap/src/render/render_header_lines.md)

# Called by

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)