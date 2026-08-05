---
type: Rust Function
title: resolve_body_part
resource: crates/lpe-imap/src/render.rs#L965-L986
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/attachment_parts
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-imap/src/render/render_part_section
  - functions/crates/lpe-imap/src/render/render_part_mime_header
---

# Signature

`fn resolve_body_part<'a>(email: &'a ImapEmail, part_path: &str) -> Option<ResolvedBodyPart<'a>>`

# Calls

- [attachment_parts](../../../../../functions/crates/lpe-imap/src/render/attachment_parts.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [render_part_section](../../../../../functions/crates/lpe-imap/src/render/render_part_section.md)
- [render_part_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_part_mime_header.md)