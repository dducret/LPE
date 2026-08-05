---
type: Rust Function
title: fallback_address
resource: crates/lpe-imap/src/render.rs#L1057-L1064
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/render_envelope
  - functions/crates/lpe-imap/src/render/render_single_address
  - functions/crates/lpe-imap/src/render/render_address_header
---

# Signature

`fn fallback_address(address: &str) -> &str`

# Called by

- [render_envelope](../../../../../functions/crates/lpe-imap/src/render/render_envelope.md)
- [render_single_address](../../../../../functions/crates/lpe-imap/src/render/render_single_address.md)
- [render_address_header](../../../../../functions/crates/lpe-imap/src/render/render_address_header.md)