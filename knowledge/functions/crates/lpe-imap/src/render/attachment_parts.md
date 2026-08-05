---
type: Rust Function
title: attachment_parts
resource: crates/lpe-imap/src/render.rs#L792-L798
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/imap_attachment_part
  called_by:
  - functions/crates/lpe-imap/src/render/render_mixed_body
  - functions/crates/lpe-imap/src/render/render_bodystructure
  - functions/crates/lpe-imap/src/render/resolve_body_part
---

# Signature

`fn attachment_parts(email: &ImapEmail) -> Vec<&ImapMimePart>`

# Calls

- [imap_attachment_part](../../../../../functions/crates/lpe-imap/src/render/imap_attachment_part.md)

# Called by

- [render_mixed_body](../../../../../functions/crates/lpe-imap/src/render/render_mixed_body.md)
- [render_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_bodystructure.md)
- [resolve_body_part](../../../../../functions/crates/lpe-imap/src/render/resolve_body_part.md)