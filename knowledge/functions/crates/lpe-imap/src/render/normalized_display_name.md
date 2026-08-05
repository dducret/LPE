---
type: Rust Function
title: normalized_display_name
resource: crates/lpe-imap/src/render.rs#L1066-L1073
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/render_envelope
  - functions/crates/lpe-imap/src/render/render_address_header
---

# Signature

`fn normalized_display_name<'a>(display_name: Option<&'a str>, address: &str) -> Option<&'a str>`

# Called by

- [render_envelope](../../../../../functions/crates/lpe-imap/src/render/render_envelope.md)
- [render_address_header](../../../../../functions/crates/lpe-imap/src/render/render_address_header.md)