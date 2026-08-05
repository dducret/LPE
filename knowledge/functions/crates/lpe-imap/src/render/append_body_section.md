---
type: Rust Function
title: append_body_section
resource: crates/lpe-imap/src/render.rs#L324-L336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/normalize_body_section
  - functions/crates/lpe-imap/src/render/render_header
  - functions/crates/lpe-imap/src/render/is_header_fields_section
  - functions/crates/lpe-imap/src/render/render_header_fields
  - functions/crates/lpe-imap/src/render/render_message_body
  - functions/crates/lpe-imap/src/render/render_full_message
  - functions/crates/lpe-imap/src/render/render_root_mime_header
  - functions/crates/lpe-imap/src/render/render_part_section
  - functions/crates/lpe-imap/src/render/apply_partial
  - functions/crates/lpe-imap/src/render/append_literal
  - functions/crates/lpe-imap/src/render/section_label
  called_by:
  - functions/crates/lpe-imap/src/render/render_fetch_response
---

# Signature

`fn append_body_section(output: &mut Vec<u8>, email: &ImapEmail, section: &BodySectionFetch)`

# Calls

- [normalize_body_section](../../../../../functions/crates/lpe-imap/src/render/normalize_body_section.md)
- [render_header](../../../../../functions/crates/lpe-imap/src/render/render_header.md)
- [is_header_fields_section](../../../../../functions/crates/lpe-imap/src/render/is_header_fields_section.md)
- [render_header_fields](../../../../../functions/crates/lpe-imap/src/render/render_header_fields.md)
- [render_message_body](../../../../../functions/crates/lpe-imap/src/render/render_message_body.md)
- [render_full_message](../../../../../functions/crates/lpe-imap/src/render/render_full_message.md)
- [render_root_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_root_mime_header.md)
- [render_part_section](../../../../../functions/crates/lpe-imap/src/render/render_part_section.md)
- [apply_partial](../../../../../functions/crates/lpe-imap/src/render/apply_partial.md)
- [append_literal](../../../../../functions/crates/lpe-imap/src/render/append_literal.md)
- [section_label](../../../../../functions/crates/lpe-imap/src/render/section_label.md)

# Called by

- [render_fetch_response](../../../../../functions/crates/lpe-imap/src/render/render_fetch_response.md)