---
type: Rust Function
title: split_content_type
resource: crates/lpe-imap/src/render.rs#L820-L833
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-imap/src/render/imap_media_token
  called_by:
  - functions/crates/lpe-imap/src/render/render_attachment_bodystructure
---

# Signature

`fn split_content_type(content_type: &str) -> (String, String)`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [imap_media_token](../../../../../functions/crates/lpe-imap/src/render/imap_media_token.md)

# Called by

- [render_attachment_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_attachment_bodystructure.md)