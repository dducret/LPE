---
type: Rust Function
title: render_body_bodystructure
resource: crates/lpe-imap/src/render.rs#L732-L748
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/render_text_bodystructure
  - functions/crates/lpe-imap/src/render/body_part_charset
  called_by:
  - functions/crates/lpe-imap/src/render/render_bodystructure
---

# Signature

`fn render_body_bodystructure(email: &ImapEmail) -> String`

# Calls

- [render_text_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_text_bodystructure.md)
- [body_part_charset](../../../../../functions/crates/lpe-imap/src/render/body_part_charset.md)

# Called by

- [render_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_bodystructure.md)