---
type: Rust Function
title: render_address_header
resource: crates/lpe-imap/src/render.rs#L1045-L1055
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/fallback_address
  - functions/crates/lpe-imap/src/render/normalized_display_name
  called_by:
  - functions/crates/lpe-imap/src/render/render_recipient_header
  - functions/crates/lpe-imap/src/search/searchable_sender
---

# Signature

`pub(crate) fn render_address_header(display_name: Option<&str>, address: &str) -> String`

# Calls

- [fallback_address](../../../../../functions/crates/lpe-imap/src/render/fallback_address.md)
- [normalized_display_name](../../../../../functions/crates/lpe-imap/src/render/normalized_display_name.md)

# Called by

- [render_recipient_header](../../../../../functions/crates/lpe-imap/src/render/render_recipient_header.md)
- [searchable_sender](../../../../../functions/crates/lpe-imap/src/search/searchable_sender.md)