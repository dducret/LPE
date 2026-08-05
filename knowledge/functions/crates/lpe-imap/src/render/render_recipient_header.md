---
type: Rust Function
title: render_recipient_header
resource: crates/lpe-imap/src/render.rs#L1035-L1043
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/render_address_header
  called_by:
  - functions/crates/lpe-imap/src/search/searchable_recipients
---

# Signature

`pub(crate) fn render_recipient_header(recipients: &[JmapEmailAddress]) -> String`

# Calls

- [render_address_header](../../../../../functions/crates/lpe-imap/src/render/render_address_header.md)

# Called by

- [searchable_recipients](../../../../../functions/crates/lpe-imap/src/search/searchable_recipients.md)